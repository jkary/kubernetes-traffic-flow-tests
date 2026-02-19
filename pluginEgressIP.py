import ipaddress
import json
import re
import typing

from typing import Optional
from typing import TYPE_CHECKING

from ktoolbox import common
from ktoolbox import host
from ktoolbox import kjinja2

import pluginbase
import task
import tftbase

from task import PluginTask
from task import TaskOperation
from testSettings import TestSettings
from tftbase import BaseOutput
from tftbase import ClusterMode
from tftbase import PluginOutput
from tftbase import TaskRole

if TYPE_CHECKING:
    import testConfig

logger = common.ExtendedLogger("tft." + __name__)


class PluginEgressIP(pluginbase.Plugin):
    PLUGIN_NAME = "egressip"

    def _enable(
        self,
        *,
        ts: TestSettings,
        perf_server: task.ServerTask,
        perf_client: task.ClientTask,
        tenant: bool,
        plugin_config: "testConfig.ConfPlugin",
    ) -> list[PluginTask]:
        # Extract and validate egress_ip parameter (required)
        egress_ip = plugin_config.params.get("egress_ip")
        if not egress_ip:
            raise ValueError(
                f"Plugin '{self.PLUGIN_NAME}' requires 'egress_ip' parameter. "
                f"Example: plugins: [{{name: egressip, egress_ip: 192.168.1.100}}]"
            )

        # Validate egress_ip is a valid IP address
        try:
            ipaddress.ip_address(egress_ip)
        except ValueError as e:
            raise ValueError(
                f"Plugin '{self.PLUGIN_NAME}': egress_ip '{egress_ip}' is not a valid IP address: {e}"
            )

        # Extract optional external_ip parameter (target IP for the test)
        external_ip = plugin_config.params.get("external_ip")
        if external_ip:
            # Validate external_ip is a valid IP address
            try:
                ipaddress.ip_address(external_ip)
            except ValueError as e:
                raise ValueError(
                    f"Plugin '{self.PLUGIN_NAME}': external_ip '{external_ip}' is not a valid IP address: {e}"
                )
            # Set the external IP override on TestSettings
            ts.set_external_ip(external_ip)
            logger.info(f"Using configured external_ip: {external_ip}")

        # Extract optional egress_interface parameter (default: auto-detect)
        egress_interface = plugin_config.params.get("egress_interface", "auto")

        # Extract optional egress_node parameter (default: client node)
        # This enables cross-node egress testing where the egress node differs
        # from the node where the client pod runs
        egress_node = plugin_config.params.get("egress_node")
        if egress_node:
            logger.info(f"Using explicit egress_node: {egress_node}")
        else:
            egress_node = ts.node_client.name
            logger.info(f"Using client node as egress_node: {egress_node}")

        # Extract optional negative_test parameter (default: false)
        # When true, adds a test to verify non-matching pods use regular node IP
        negative_test = plugin_config.params.get("negative_test", False)
        if isinstance(negative_test, str):
            negative_test = negative_test.lower() in ("true", "yes", "1")

        # Extract optional verify_dpu_offload parameter (default: false)
        # When true, verifies SNAT happens on DPU, not host
        verify_dpu_offload = plugin_config.params.get("verify_dpu_offload", False)
        if isinstance(verify_dpu_offload, str):
            verify_dpu_offload = verify_dpu_offload.lower() in ("true", "yes", "1")

        if verify_dpu_offload and ts.cfg_descr.tc.mode != ClusterMode.DPU:
            raise ValueError(
                f"Plugin '{self.PLUGIN_NAME}': verify_dpu_offload requires DPU mode. "
                f"Set kubeconfig_infra in the configuration."
            )

        tasks: list[PluginTask] = [
            TaskEgressIPSetup(
                ts, perf_server, perf_client, tenant, egress_ip, egress_node
            ),
            TaskEgressIPVerify(
                ts,
                perf_server,
                perf_client,
                tenant,
                egress_ip,
                egress_interface,
                egress_node,
            ),
        ]

        if negative_test:
            logger.info("Adding negative test to verify non-matching pods use node IP")
            tasks.append(
                TaskEgressIPVerifyNegative(
                    ts,
                    perf_server,
                    perf_client,
                    tenant,
                    egress_ip,
                    egress_interface,
                    egress_node,
                )
            )

        if verify_dpu_offload:
            logger.info("Adding DPU offload verification task")
            tasks.append(
                TaskEgressIPVerifyDPUOffload(
                    ts,
                    perf_server,
                    perf_client,
                    tenant,
                    egress_ip,
                    egress_interface,
                    egress_node,
                )
            )

        return tasks


plugin = pluginbase.register_plugin(PluginEgressIP())


class TaskEgressIPSetup(PluginTask):
    """Task to set up EgressIP CRD and label the egress node."""

    @property
    def plugin(self) -> pluginbase.Plugin:
        return plugin

    @property
    def _is_dpu_mode(self) -> bool:
        return self.tc.mode == ClusterMode.DPU

    def __init__(
        self,
        ts: TestSettings,
        perf_server: task.ServerTask,
        perf_client: task.ClientTask,
        tenant: bool,
        egress_ip: str,
        egress_node: str,
    ):
        super().__init__(
            ts=ts,
            index=0,
            task_role=TaskRole.SERVER,
            tenant=tenant,
        )

        self._perf_server = perf_server
        self._perf_client = perf_client

        # Get egress configuration - egress_node can differ from client node
        # for cross-node egress testing
        self._egress_node = egress_node
        self._egress_ip = egress_ip
        self._egress_ips: tuple[str, ...] = (egress_ip,)

        # The EgressIP is a cluster-scoped resource
        self.pod_name = f"egressip-setup-{tftbase.str_sanitize(self._egress_node)}"
        self.in_file_template = ""  # No pod needed for setup task

        logger.info(
            f"TaskEgressIPSetup: egress_node={self._egress_node}, "
            f"egress_ip={self._egress_ip}, dpu_mode={self._is_dpu_mode}"
        )

    def initialize(self) -> None:
        super().initialize()
        # Validate egress IP is in node's subnet
        self._validate_egress_ip_in_subnet()
        # Validate egress IP is not in use (doesn't respond to ping)
        self._validate_egress_ip_not_in_use()
        # Label the egress node
        self._label_egress_node()
        # Create the EgressIP CRD
        self._create_egressip_crd()

    def _get_node_network_info(self) -> tuple[str, str]:
        """Get the node's primary IP and network CIDR.

        Returns (node_ip, cidr) where cidr is like '192.168.1.0/24'.
        """
        egress_node = self._egress_node

        # In DPU mode, query the DPU cluster for node info
        if self._is_dpu_mode:
            dpu_node = self._get_dpu_node_name(egress_node)
            result = self.tc.client_infra.oc(
                f"get node {dpu_node} -o jsonpath='{{.status.addresses[?(@.type==\"InternalIP\")].address}}'",
                may_fail=True,
            )
        else:
            result = self.tc.client_tenant.oc(
                f"get node {egress_node} -o jsonpath='{{.status.addresses[?(@.type==\"InternalIP\")].address}}'",
                may_fail=True,
            )

        if not result.success or not result.out.strip().strip("'\""):
            raise RuntimeError(f"Failed to get InternalIP for node {egress_node}")

        node_ip = result.out.strip().strip("'\"")

        # Get the network CIDR by querying hostSubnet or using the node's annotations
        # For OVN-Kubernetes, check annotations for network info
        if self._is_dpu_mode:
            result = self.tc.client_infra.oc(
                f"get node {dpu_node} -o jsonpath='{{.metadata.annotations.k8s\\.ovn\\.org/node-primary-ifaddr}}'",
                may_fail=True,
            )
        else:
            result = self.tc.client_tenant.oc(
                f"get node {egress_node} -o jsonpath='{{.metadata.annotations.k8s\\.ovn\\.org/node-primary-ifaddr}}'",
                may_fail=True,
            )

        if result.success and result.out.strip().strip("'\""):
            # Format: {"ipv4":"192.168.12.126/24","ipv6":"..."}
            try:
                ifaddr = json.loads(result.out.strip().strip("'\""))
                if "ipv4" in ifaddr:
                    # Extract CIDR from address like "192.168.12.126/24"
                    addr_with_prefix = ifaddr["ipv4"]
                    network = ipaddress.ip_network(addr_with_prefix, strict=False)
                    return (node_ip, str(network))
            except (json.JSONDecodeError, KeyError, ValueError) as e:
                logger.warning(f"Failed to parse node-primary-ifaddr: {e}")

        # Fallback: assume /24 network
        try:
            ip = ipaddress.ip_address(node_ip)
            if isinstance(ip, ipaddress.IPv4Address):
                network = ipaddress.ip_network(f"{node_ip}/24", strict=False)
                return (node_ip, str(network))
        except ValueError:
            pass

        raise RuntimeError(f"Could not determine network CIDR for node {egress_node}")

    def _validate_egress_ip_in_subnet(self) -> None:
        """Validate that the egress IP is in the node's subnet."""
        node_ip, network_cidr = self._get_node_network_info()

        try:
            network = ipaddress.ip_network(network_cidr, strict=False)
            egress_addr = ipaddress.ip_address(self._egress_ip)

            if egress_addr not in network:
                raise ValueError(
                    f"EgressIP '{self._egress_ip}' is not in node's network '{network_cidr}'. "
                    f"The egress IP must be in the same subnet as the egress node ({self._egress_node}, IP: {node_ip})."
                )

            logger.info(
                f"Validated egress IP {self._egress_ip} is in node network {network_cidr}"
            )
        except ValueError as e:
            raise ValueError(f"EgressIP validation failed: {e}")

    def _validate_egress_ip_not_in_use(self) -> None:
        """Validate that the egress IP is not responding to ping.

        Skip validation if the EgressIP CRD already exists (e.g., from a
        previous test run that hasn't been cleaned up yet).
        """
        # Check if EgressIP CRD already exists with this IP
        egressip_name = f"egressip-{tftbase.str_sanitize(self._egress_node)}"
        result = self.tc.client_tenant.oc(
            f"get egressip {egressip_name}",
            namespace=None,
            may_fail=True,
        )
        if result.success:
            logger.info(
                f"EgressIP CRD {egressip_name} already exists, skipping ping check"
            )
            return

        logger.info(f"Checking that egress IP {self._egress_ip} is not in use...")

        # Ping with short timeout (1 second, 2 attempts)
        result = host.local.run(
            f"ping -c 2 -W 1 {self._egress_ip}",
            check_success=lambda r: True,  # Don't fail on non-zero exit
        )

        if result.success and result.returncode == 0:
            raise ValueError(
                f"EgressIP '{self._egress_ip}' responded to ping! "
                f"The egress IP must not be in use. Choose an unused IP in the node's subnet."
            )

        logger.info(
            f"Verified egress IP {self._egress_ip} is not responding to ping (safe to use)"
        )

    def _create_egressip_crd(self) -> None:
        """Create the EgressIP CRD."""
        namespace = self.get_namespace()
        egressip_name = f"egressip-{tftbase.str_sanitize(self._egress_node)}"

        # Build the egress IPs YAML list
        egress_ips_yaml = "\n".join(f"      - {ip}" for ip in self._egress_ips)

        template_args = {
            "egressip_name": f'"{egressip_name}"',
            "label_tft_tests": f'"{self.index}"',
            "egress_ips_yaml": egress_ips_yaml,
            "name_space": f'"{namespace}"',
        }

        # Render the EgressIP CRD YAML
        egressip_yaml_path = tftbase.get_manifest_renderpath(f"{egressip_name}.yaml")
        egressip_template = tftbase.get_manifest("egressip.yaml.j2")
        kjinja2.render_file(
            egressip_template,
            template_args,
            out_file=egressip_yaml_path,
        )
        logger.info(f"EgressIP CRD YAML rendered to {egressip_yaml_path}")

        # Apply the EgressIP CRD (cluster-scoped, so no namespace)
        result = self.tc.client_tenant.oc(
            f"apply -f {egressip_yaml_path}",
            namespace=None,
            die_on_error=True,
        )
        logger.info(f"EgressIP CRD created: {result.out}")

    def _label_egress_node(self) -> None:
        """Label the node as egress-assignable."""
        egress_node = self._egress_node

        # In DPU mode, label the DPU node, not the host node
        if self._is_dpu_mode:
            dpu_node = self._get_dpu_node_name(egress_node)
            logger.info(
                f"DPU mode: labeling DPU node {dpu_node} instead of host node {egress_node}"
            )
            self.tc.client_infra.oc(
                f"label node {dpu_node} k8s.ovn.org/egress-assignable=true --overwrite",
                namespace=None,
                die_on_error=True,
            )
        else:
            logger.info(f"Labeling node {egress_node} as egress-assignable")
            self.tc.client_tenant.oc(
                f"label node {egress_node} k8s.ovn.org/egress-assignable=true --overwrite",
                namespace=None,
                die_on_error=True,
            )

    def _get_dpu_node_name(self, host_node: str) -> str:
        """Get the DPU node name corresponding to a host node."""
        host_label = self.tc.dpu_node_host_label
        if not host_label:
            raise ValueError(
                "dpu_node_host_label must be configured when running in DPU mode"
            )

        selector = f"{host_label}={host_node}"
        result = self.tc.client_infra.oc(
            f"get nodes -l {selector} -o jsonpath='{{.items[*].metadata.name}}'",
            may_fail=True,
        )

        if not result.success or not result.out.strip().strip("'\""):
            raise RuntimeError(f"No DPU node found with label {selector}")

        dpu_nodes = result.out.strip().strip("'\"").split()
        return dpu_nodes[0]

    def _create_setup_operation(self) -> Optional[TaskOperation]:
        # Setup is done in initialize() - no pod to create
        return None

    def _create_task_operation(self) -> Optional[TaskOperation]:
        def _thread_action() -> BaseOutput:
            # Must participate in barrier even though setup is already done
            self.ts.clmo_barrier.wait()
            return PluginOutput(
                success=True,
                msg="EgressIP setup completed",
                plugin_metadata=self.get_plugin_metadata(),
                command="",
                result={},
            )

        return TaskOperation(
            log_name=self.log_name,
            thread_action=_thread_action,
        )


class TaskEgressIPVerify(PluginTask):
    """Task to verify EgressIP is working by running tcpdump on the egress node."""

    @property
    def plugin(self) -> pluginbase.Plugin:
        return plugin

    @property
    def _is_dpu_mode(self) -> bool:
        return self.tc.mode == ClusterMode.DPU

    def __init__(
        self,
        ts: TestSettings,
        perf_server: task.ServerTask,
        perf_client: task.ClientTask,
        tenant: bool,
        egress_ip: str,
        egress_interface: str,
        egress_node: str,
    ):
        super().__init__(
            ts=ts,
            index=0,
            task_role=TaskRole.CLIENT,
            tenant=tenant,
        )

        self._perf_server = perf_server
        self._perf_client = perf_client

        # Get egress configuration - egress_node can differ from client node
        # for cross-node egress testing
        self._egress_node = egress_node
        self._egress_ip = egress_ip
        self._egress_ips: tuple[str, ...] = (egress_ip,)
        self._egress_interface = egress_interface

        # tcpdump pod runs on the egress node (DPU node in DPU mode)
        self._tcpdump_pod_name: Optional[str] = None
        self._tcpdump_node_name: Optional[str] = None

        # For verification, we'll use the host-backed pod approach
        self.pod_name = f"egressip-verify-{tftbase.str_sanitize(self._egress_node)}"
        self.in_file_template = tftbase.get_manifest("tcpdump-pod.yaml.j2")

        logger.info(
            f"TaskEgressIPVerify: egress_node={self._egress_node}, "
            f"egress_ip={self._egress_ip}, egress_interface={self._egress_interface}, "
            f"dpu_mode={self._is_dpu_mode}"
        )

    def get_template_args(self) -> dict[str, str | list[str] | bool]:
        args = super().get_template_args()
        # Override for tcpdump pod - must run on egress node, not client node
        args["node_name"] = f'"{self._egress_node}"'
        args["command"] = json.dumps(["/usr/bin/container-entry-point.sh"])
        args["args"] = json.dumps(["sleep", "infinity"])
        return args

    def initialize(self) -> None:
        super().initialize()
        self.render_pod_file("Tcpdump Pod Yaml")

        if self._is_dpu_mode:
            # In DPU mode, create tcpdump pod on the DPU cluster
            self._tcpdump_node_name = self._get_dpu_node_name(self._egress_node)
            self._tcpdump_pod_name = (
                f"tcpdump-dpu-{tftbase.str_sanitize(self._tcpdump_node_name)}"
            )
            self._initialize_dpu_tcpdump_pod()
        else:
            # In single-cluster mode, use the regular pod
            self._tcpdump_node_name = self._egress_node
            self._tcpdump_pod_name = self.pod_name

    def _get_dpu_node_name(self, host_node: str) -> str:
        """Get the DPU node name corresponding to a host node."""
        host_label = self.tc.dpu_node_host_label
        if not host_label:
            raise ValueError(
                "dpu_node_host_label must be configured when running in DPU mode"
            )

        selector = f"{host_label}={host_node}"
        result = self.tc.client_infra.oc(
            f"get nodes -l {selector} -o jsonpath='{{.items[*].metadata.name}}'",
            may_fail=True,
        )

        if not result.success or not result.out.strip().strip("'\""):
            raise RuntimeError(f"No DPU node found with label {selector}")

        dpu_nodes = result.out.strip().strip("'\"").split()
        return dpu_nodes[0]

    def _initialize_dpu_tcpdump_pod(self) -> None:
        """Create tcpdump pod on the DPU cluster."""
        assert self._tcpdump_pod_name is not None
        assert self._tcpdump_node_name is not None

        logger.info(
            f"Creating DPU tcpdump pod {self._tcpdump_pod_name} "
            f"on node {self._tcpdump_node_name}"
        )

        namespace = self.get_namespace()
        template_args = {
            "name_space": f'"{namespace}"',
            "test_image": f'"{tftbase.get_tft_test_image()}"',
            "image_pull_policy": f'"{tftbase.get_tft_image_pull_policy()}"',
            "command": '["/usr/bin/container-entry-point.sh"]',
            "args": '["sleep", "infinity"]',
            "label_tft_tests": f'"{self.index}"',
            "node_name": f'"{self._tcpdump_node_name}"',
            "pod_name": f'"{self._tcpdump_pod_name}"',
        }

        # Render the pod YAML
        dpu_pod_yaml_path = tftbase.get_manifest_renderpath(
            self._tcpdump_pod_name + ".yaml"
        )
        kjinja2.render_file(
            self.in_file_template,
            template_args,
            out_file=dpu_pod_yaml_path,
        )
        logger.info(f"DPU tcpdump pod YAML rendered to {dpu_pod_yaml_path}")

        # Apply the pod on the infra (DPU) cluster
        result = self.tc.client_infra.oc(
            f"apply -f {dpu_pod_yaml_path}",
            namespace=namespace,
            may_fail=True,
        )
        if not result.success:
            raise RuntimeError(f"Failed to create DPU tcpdump pod: {result.err}")

        # Wait for the pod to be ready
        result = self.tc.client_infra.oc(
            f"wait --for=condition=Ready pod/{self._tcpdump_pod_name} --timeout=60s",
            namespace=namespace,
            may_fail=True,
        )
        if not result.success:
            raise RuntimeError(f"DPU tcpdump pod failed to become ready: {result.err}")

        logger.info(f"DPU tcpdump pod {self._tcpdump_pod_name} is ready")

    def _run_tcpdump_cmd(
        self,
        cmd: str,
        *,
        may_fail: bool = False,
    ) -> host.Result:
        """Run a command on the tcpdump pod."""
        if self._is_dpu_mode:
            assert self._tcpdump_pod_name is not None
            return self.tc.client_infra.oc_exec(
                cmd,
                pod_name=self._tcpdump_pod_name,
                may_fail=may_fail,
                namespace=self.get_namespace(),
            )
        else:
            return self.run_oc_exec(cmd, may_fail=may_fail)

    def _get_default_interface(self) -> Optional[str]:
        """Get the physical network interface for egress traffic.

        For OVN-Kubernetes, we need to find the physical interface attached
        to br-ex, not br-ex itself (tcpdump can't see forwarded traffic on
        OVS bridge internal ports).
        """
        # First, try to get the physical port from br-ex using OVS
        r = self._run_tcpdump_cmd(
            "ovs-vsctl list-ports br-ex 2>/dev/null | grep -v ^patch | head -1",
            may_fail=True,
        )
        if r.success and r.out.strip():
            port = r.out.strip().split()[0]
            logger.debug(f"Found physical port on br-ex: {port}")
            return port

        # Try to find a physical ethernet interface attached to OVS
        # On OVN-Kubernetes, the physical interface on br-ex has master=ovs-system
        # Physical interfaces: eth*, eno*, ens*, enp* (standard)
        # DPU interfaces: p0, p1, pf0hpf, enp*s*f* (BlueField)
        r = self._run_tcpdump_cmd(
            "ip -j link show 2>/dev/null",
            may_fail=True,
        )
        if r.success and r.out.strip():
            try:
                links = json.loads(r.out.strip())
                for link in links:
                    name = link.get("ifname", "")
                    operstate = link.get("operstate", "")
                    master = link.get("master", "")

                    # Must be UP and attached to OVS (master=ovs-system)
                    if operstate != "UP" or master != "ovs-system":
                        continue

                    # Skip VF representors (contain "npX" suffix like enp3s0f0np0)
                    if re.search(r"np\d+$", name):
                        continue

                    # Match physical interface naming patterns
                    if (
                        name.startswith(("eth", "eno", "ens", "enp"))
                        or re.match(r"^p\d+$", name)  # DPU: p0, p1
                        or re.match(r"^pf\d+hpf$", name)  # DPU: pf0hpf
                    ):
                        logger.debug(f"Found physical interface on OVS: {name}")
                        return typing.cast(str, name)
            except (json.JSONDecodeError, KeyError):
                pass

        # Fallback: get interface from default route
        r = self._run_tcpdump_cmd("ip -j route get 1", may_fail=True)
        if r.success and r.out.strip():
            try:
                routes = json.loads(r.out.strip())
                if routes and "dev" in routes[0]:
                    ifname: str = routes[0]["dev"]
                    logger.debug(f"Default interface from route: {ifname}")
                    return ifname
            except (json.JSONDecodeError, KeyError, IndexError):
                pass

        logger.warning("Could not determine default interface")
        return None

    def _create_task_operation(self) -> TaskOperation:
        def _thread_action() -> BaseOutput:
            success_result = True
            msg: Optional[str] = None
            parsed_data: dict[str, typing.Any] = {}

            # Wait for server to be alive first
            self.ts.clmo_barrier.wait()

            # Get the target (server) IP to filter traffic
            target_ip = self._perf_client.get_target_ip()
            target_port = self._perf_client.get_target_port()

            logger.info(
                f"EgressIP verification: monitoring traffic to {target_ip}:{target_port}"
            )

            # Use configured interface or auto-detect for capturing OVN egress traffic
            if self._egress_interface == "auto":
                ifname = self._get_default_interface()
                if not ifname:
                    ifname = "br-ex"  # Fallback to br-ex
                logger.info(f"Auto-detected egress interface: {ifname}")
            else:
                ifname = self._egress_interface

            parsed_data["target_ip"] = target_ip
            parsed_data["target_port"] = target_port
            parsed_data["interface"] = ifname

            # Start tcpdump to capture packets going to the target
            # Filter: outbound TCP traffic to target IP/port
            # -c 10: Capture first 10 packets
            # -n: Don't resolve hostnames
            # -q: Quiet output
            # We capture packets and look at the source IP
            tcpdump_filter = f"tcp and dst host {target_ip} and dst port {target_port}"
            tcpdump_cmd = (
                f"timeout 30 tcpdump -i {ifname} -n -c 10 '{tcpdump_filter}' 2>&1"
            )

            logger.info(f"Running tcpdump: {tcpdump_cmd}")
            r = self._run_tcpdump_cmd(tcpdump_cmd, may_fail=True)

            parsed_data["tcpdump_output"] = r.out
            parsed_data["tcpdump_success"] = r.success

            # Wait for client to finish
            self.ts.event_client_finished.wait()

            if not r.success and "0 packets captured" not in r.out:
                # tcpdump may fail with timeout if no packets, that's OK
                if r.returncode != 124:  # timeout exit code
                    success_result = False
                    msg = f"tcpdump failed: {r.err}"
                    logger.error(msg)

            # Parse tcpdump output to extract source IPs
            source_ips = self._parse_tcpdump_output(r.out)
            parsed_data["source_ips"] = list(source_ips)
            parsed_data["expected_egress_ip"] = self._egress_ip

            logger.info(f"Captured source IPs: {source_ips}")
            logger.info(f"Expected egress IP: {self._egress_ip}")

            # Verify that the traffic is using the expected EgressIP as source
            if not source_ips:
                # No packets captured - could be a connectivity issue
                success_result = False
                msg = (
                    f"No traffic captured for EgressIP verification. "
                    f"Expected to see source IP {self._egress_ip}"
                )
                logger.warning(msg)
            elif self._egress_ip in source_ips:
                # Success: traffic is using the configured EgressIP
                msg = (
                    f"EgressIP verification PASSED: traffic is using "
                    f"egress IP {self._egress_ip}"
                )
                logger.info(msg)
            else:
                # Traffic is not using the expected EgressIP
                success_result = False
                msg = (
                    f"EgressIP verification FAILED: expected source IP "
                    f"{self._egress_ip}, but captured: {source_ips}"
                )
                logger.error(msg)

            return PluginOutput(
                success=success_result,
                msg=msg,
                plugin_metadata=self.get_plugin_metadata(),
                command=tcpdump_cmd,
                result=parsed_data,
            )

        return TaskOperation(
            log_name=self.log_name,
            thread_action=_thread_action,
        )

    def _parse_tcpdump_output(self, output: str) -> set[str]:
        """Parse tcpdump output to extract source IPs.

        tcpdump output format:
        12:34:56.789 IP 192.168.1.100.54321 > 10.0.0.1.5201: Flags [S], ...
        """
        source_ips: set[str] = set()

        # Pattern to match: IP <src_ip>.<port> > <dst_ip>.<port>:
        # The source IP is the first IP address after "IP "
        pattern = r"IP\s+(\d+\.\d+\.\d+\.\d+)\.\d+\s+>"

        for line in output.splitlines():
            match = re.search(pattern, line)
            if match:
                source_ips.add(match.group(1))

        return source_ips

    def _aggregate_output_log_success(
        self,
        result: tftbase.AggregatableOutput,
    ) -> None:
        assert isinstance(result, PluginOutput)
        logger.info(f"EgressIP verification results: {result.result}")


class TaskEgressIPVerifyNegative(PluginTask):
    """Task to verify pods without matching labels use regular node IP, not EgressIP.

    This negative test ensures that the EgressIP podSelector is working correctly
    by verifying that pods NOT matching the selector still use the node's regular IP.
    """

    @property
    def plugin(self) -> pluginbase.Plugin:
        return plugin

    @property
    def _is_dpu_mode(self) -> bool:
        return self.tc.mode == ClusterMode.DPU

    def __init__(
        self,
        ts: TestSettings,
        perf_server: task.ServerTask,
        perf_client: task.ClientTask,
        tenant: bool,
        egress_ip: str,
        egress_interface: str,
        egress_node: str,
    ):
        super().__init__(
            ts=ts,
            index=1,  # Different index to avoid name conflicts
            task_role=TaskRole.CLIENT,
            tenant=tenant,
        )

        self._perf_server = perf_server
        self._perf_client = perf_client
        self._egress_node = egress_node
        self._egress_ip = egress_ip
        self._egress_interface = egress_interface

        # Pod for negative test (no tft-tests label)
        self._negative_pod_name = (
            f"egressip-negative-{tftbase.str_sanitize(egress_node)}"
        )
        self.pod_name = self._negative_pod_name
        self.in_file_template = tftbase.get_manifest("pod-no-label.yaml.j2")

        # tcpdump pod runs on the egress node
        self._tcpdump_pod_name: Optional[str] = None
        self._tcpdump_node_name: Optional[str] = None

        logger.info(
            f"TaskEgressIPVerifyNegative: egress_node={self._egress_node}, "
            f"egress_ip={self._egress_ip}, negative_pod={self._negative_pod_name}"
        )

    def _get_node_ip(self, node_name: str) -> str:
        """Get the InternalIP of a node."""
        result = self.tc.client_tenant.oc(
            f"get node {node_name} -o jsonpath='{{.status.addresses[?(@.type==\"InternalIP\")].address}}'",
            may_fail=True,
        )

        if not result.success or not result.out.strip().strip("'\""):
            raise RuntimeError(f"Failed to get InternalIP for node {node_name}")

        return result.out.strip().strip("'\"")

    def get_template_args(self) -> dict[str, str | list[str] | bool]:
        args = super().get_template_args()
        # Override for negative test pod - simple curl container
        args["command"] = json.dumps(["/usr/bin/container-entry-point.sh"])
        args["args"] = json.dumps(["sleep", "infinity"])
        return args

    def initialize(self) -> None:
        super().initialize()
        self.render_pod_file("Negative Test Pod Yaml")

        # Set up tcpdump pod on the egress node
        if self._is_dpu_mode:
            self._tcpdump_node_name = self._get_dpu_node_name(self._egress_node)
            self._tcpdump_pod_name = (
                f"tcpdump-neg-{tftbase.str_sanitize(self._tcpdump_node_name)}"
            )
            self._initialize_dpu_tcpdump_pod()
        else:
            self._tcpdump_node_name = self._egress_node
            # Reuse the main tcpdump pod if on same node
            self._tcpdump_pod_name = (
                f"egressip-verify-{tftbase.str_sanitize(self._egress_node)}"
            )

    def _get_dpu_node_name(self, host_node: str) -> str:
        """Get the DPU node name corresponding to a host node."""
        host_label = self.tc.dpu_node_host_label
        if not host_label:
            raise ValueError(
                "dpu_node_host_label must be configured when running in DPU mode"
            )

        selector = f"{host_label}={host_node}"
        result = self.tc.client_infra.oc(
            f"get nodes -l {selector} -o jsonpath='{{.items[*].metadata.name}}'",
            may_fail=True,
        )

        if not result.success or not result.out.strip().strip("'\""):
            raise RuntimeError(f"No DPU node found with label {selector}")

        dpu_nodes = result.out.strip().strip("'\"").split()
        return dpu_nodes[0]

    def _initialize_dpu_tcpdump_pod(self) -> None:
        """Create tcpdump pod on the DPU cluster for negative test."""
        assert self._tcpdump_pod_name is not None
        assert self._tcpdump_node_name is not None

        logger.info(
            f"Creating DPU tcpdump pod {self._tcpdump_pod_name} "
            f"on node {self._tcpdump_node_name} for negative test"
        )

        namespace = self.get_namespace()
        template_args = {
            "name_space": f'"{namespace}"',
            "test_image": f'"{tftbase.get_tft_test_image()}"',
            "image_pull_policy": f'"{tftbase.get_tft_image_pull_policy()}"',
            "command": '["/usr/bin/container-entry-point.sh"]',
            "args": '["sleep", "infinity"]',
            "label_tft_tests": f'"{self.index}"',
            "node_name": f'"{self._tcpdump_node_name}"',
            "pod_name": f'"{self._tcpdump_pod_name}"',
        }

        tcpdump_template = tftbase.get_manifest("tcpdump-pod.yaml.j2")
        dpu_pod_yaml_path = tftbase.get_manifest_renderpath(
            self._tcpdump_pod_name + ".yaml"
        )
        kjinja2.render_file(
            tcpdump_template,
            template_args,
            out_file=dpu_pod_yaml_path,
        )

        result = self.tc.client_infra.oc(
            f"apply -f {dpu_pod_yaml_path}",
            namespace=namespace,
            may_fail=True,
        )
        if not result.success:
            raise RuntimeError(f"Failed to create DPU tcpdump pod: {result.err}")

        result = self.tc.client_infra.oc(
            f"wait --for=condition=Ready pod/{self._tcpdump_pod_name} --timeout=60s",
            namespace=namespace,
            may_fail=True,
        )
        if not result.success:
            raise RuntimeError(f"DPU tcpdump pod failed to become ready: {result.err}")

        logger.info(f"DPU tcpdump pod {self._tcpdump_pod_name} is ready")

    def _run_tcpdump_cmd(
        self,
        cmd: str,
        *,
        may_fail: bool = False,
    ) -> host.Result:
        """Run a command on the tcpdump pod."""
        if self._is_dpu_mode:
            assert self._tcpdump_pod_name is not None
            return self.tc.client_infra.oc_exec(
                cmd,
                pod_name=self._tcpdump_pod_name,
                may_fail=may_fail,
                namespace=self.get_namespace(),
            )
        else:
            # In single-cluster mode, use the main verification pod
            assert self._tcpdump_pod_name is not None
            return self.tc.client_tenant.oc_exec(
                cmd,
                pod_name=self._tcpdump_pod_name,
                may_fail=may_fail,
                namespace=self.get_namespace(),
            )

    def _get_default_interface(self) -> Optional[str]:
        """Get the physical network interface for egress traffic."""
        r = self._run_tcpdump_cmd(
            "ovs-vsctl list-ports br-ex 2>/dev/null | grep -v ^patch | head -1",
            may_fail=True,
        )
        if r.success and r.out.strip():
            return r.out.strip().split()[0]

        r = self._run_tcpdump_cmd("ip -j route get 1", may_fail=True)
        if r.success and r.out.strip():
            try:
                routes = json.loads(r.out.strip())
                if routes and "dev" in routes[0]:
                    return typing.cast(str, routes[0]["dev"])
            except (json.JSONDecodeError, KeyError, IndexError):
                pass

        return None

    def _parse_tcpdump_output(self, output: str) -> set[str]:
        """Parse tcpdump output to extract source IPs."""
        source_ips: set[str] = set()
        pattern = r"IP\s+(\d+\.\d+\.\d+\.\d+)\.\d+\s+>"

        for line in output.splitlines():
            match = re.search(pattern, line)
            if match:
                source_ips.add(match.group(1))

        return source_ips

    def _create_task_operation(self) -> TaskOperation:
        def _thread_action() -> BaseOutput:
            success_result = True
            msg: Optional[str] = None
            parsed_data: dict[str, typing.Any] = {}

            self.ts.clmo_barrier.wait()

            # Get expected node IP (traffic from non-matching pod should use this)
            expected_node_ip = self._get_node_ip(self._egress_node)
            parsed_data["expected_node_ip"] = expected_node_ip
            parsed_data["egress_ip_should_not_appear"] = self._egress_ip

            # Get target info
            target_ip = self._perf_client.get_target_ip()
            target_port = self._perf_client.get_target_port()

            logger.info(
                f"Negative test: verifying traffic from pod without label "
                f"uses node IP {expected_node_ip}, not EgressIP {self._egress_ip}"
            )

            # Determine interface
            if self._egress_interface == "auto":
                ifname = self._get_default_interface()
                if not ifname:
                    ifname = "br-ex"
            else:
                ifname = self._egress_interface

            parsed_data["interface"] = ifname

            # Run curl from negative test pod to generate traffic
            # Use a short request to an external target
            curl_cmd = f"curl -s -o /dev/null -w '%{{http_code}}' --connect-timeout 5 http://{target_ip}:{target_port}/ || true"
            self.run_oc_exec(curl_cmd, may_fail=True)

            # Capture traffic with tcpdump
            # Filter for traffic from this pod's IP range
            tcpdump_filter = f"tcp and dst host {target_ip} and dst port {target_port}"
            tcpdump_cmd = (
                f"timeout 10 tcpdump -i {ifname} -n -c 5 '{tcpdump_filter}' 2>&1"
            )

            logger.info(f"Running negative test tcpdump: {tcpdump_cmd}")
            r = self._run_tcpdump_cmd(tcpdump_cmd, may_fail=True)

            parsed_data["tcpdump_output"] = r.out
            parsed_data["tcpdump_success"] = r.success

            self.ts.event_client_finished.wait()

            source_ips = self._parse_tcpdump_output(r.out)
            parsed_data["source_ips"] = list(source_ips)

            logger.info(f"Negative test captured source IPs: {source_ips}")

            # Verify that EgressIP is NOT used
            if self._egress_ip in source_ips:
                success_result = False
                msg = (
                    f"Negative test FAILED: pod without tft-tests label is using "
                    f"EgressIP {self._egress_ip}! EgressIP podSelector may not be working."
                )
                logger.error(msg)
            elif expected_node_ip in source_ips:
                msg = (
                    f"Negative test PASSED: pod without tft-tests label is using "
                    f"node IP {expected_node_ip} (not EgressIP)"
                )
                logger.info(msg)
            elif not source_ips:
                # No packets captured - inconclusive but not a failure
                msg = "Negative test inconclusive: no traffic captured"
                logger.warning(msg)
            else:
                msg = (
                    f"Negative test PASSED: pod is not using EgressIP. "
                    f"Captured IPs: {source_ips}"
                )
                logger.info(msg)

            return PluginOutput(
                success=success_result,
                msg=msg,
                plugin_metadata=self.get_plugin_metadata(),
                command=tcpdump_cmd,
                result=parsed_data,
            )

        return TaskOperation(
            log_name=self.log_name,
            thread_action=_thread_action,
        )

    def _aggregate_output_log_success(
        self,
        result: tftbase.AggregatableOutput,
    ) -> None:
        assert isinstance(result, PluginOutput)
        logger.info(f"EgressIP negative test results: {result.result}")


class TaskEgressIPVerifyDPUOffload(PluginTask):
    """Task to verify SNAT happens on DPU, not on the host.

    In DPU mode, the EgressIP SNAT should be performed by OVN on the DPU,
    not on the host. This task runs tcpdump on the HOST to verify that
    traffic leaving the host does NOT have the EgressIP as source (meaning
    the SNAT happens later, on the DPU).
    """

    @property
    def plugin(self) -> pluginbase.Plugin:
        return plugin

    @property
    def _is_dpu_mode(self) -> bool:
        return self.tc.mode == ClusterMode.DPU

    def __init__(
        self,
        ts: TestSettings,
        perf_server: task.ServerTask,
        perf_client: task.ClientTask,
        tenant: bool,
        egress_ip: str,
        egress_interface: str,
        egress_node: str,
    ):
        super().__init__(
            ts=ts,
            index=2,  # Different index to avoid name conflicts
            task_role=TaskRole.CLIENT,
            tenant=tenant,
        )

        self._perf_server = perf_server
        self._perf_client = perf_client
        self._egress_node = egress_node
        self._egress_ip = egress_ip
        self._egress_interface = egress_interface

        # Host tcpdump pod - runs on the HOST (tenant cluster), not DPU
        self._host_tcpdump_pod_name = (
            f"egressip-host-tcpdump-{tftbase.str_sanitize(egress_node)}"
        )
        self.pod_name = self._host_tcpdump_pod_name
        self.in_file_template = tftbase.get_manifest("tcpdump-pod.yaml.j2")

        logger.info(
            f"TaskEgressIPVerifyDPUOffload: egress_node={self._egress_node}, "
            f"egress_ip={self._egress_ip}, host_pod={self._host_tcpdump_pod_name}"
        )

    def get_template_args(self) -> dict[str, str | list[str] | bool]:
        args = super().get_template_args()
        args["command"] = json.dumps(["/usr/bin/container-entry-point.sh"])
        args["args"] = json.dumps(["sleep", "infinity"])
        return args

    def initialize(self) -> None:
        super().initialize()
        self.render_pod_file("Host Tcpdump Pod Yaml for DPU Offload Verification")

    def _get_default_interface(self) -> Optional[str]:
        """Get the physical network interface on the host."""
        # On host, look for the interface connected to the DPU
        r = self.run_oc_exec(
            "ovs-vsctl list-ports br-ex 2>/dev/null | grep -v ^patch | head -1",
            may_fail=True,
        )
        if r.success and r.out.strip():
            return r.out.strip().split()[0]

        r = self.run_oc_exec("ip -j route get 1", may_fail=True)
        if r.success and r.out.strip():
            try:
                routes = json.loads(r.out.strip())
                if routes and "dev" in routes[0]:
                    return typing.cast(str, routes[0]["dev"])
            except (json.JSONDecodeError, KeyError, IndexError):
                pass

        return None

    def _parse_tcpdump_output(self, output: str) -> set[str]:
        """Parse tcpdump output to extract source IPs."""
        source_ips: set[str] = set()
        pattern = r"IP\s+(\d+\.\d+\.\d+\.\d+)\.\d+\s+>"

        for line in output.splitlines():
            match = re.search(pattern, line)
            if match:
                source_ips.add(match.group(1))

        return source_ips

    def _create_task_operation(self) -> TaskOperation:
        def _thread_action() -> BaseOutput:
            success_result = True
            msg: Optional[str] = None
            parsed_data: dict[str, typing.Any] = {}

            self.ts.clmo_barrier.wait()

            target_ip = self._perf_client.get_target_ip()
            target_port = self._perf_client.get_target_port()

            logger.info(
                f"DPU offload verification: checking that SNAT to {self._egress_ip} "
                f"does NOT happen on the host"
            )

            # Determine interface on host
            if self._egress_interface == "auto":
                ifname = self._get_default_interface()
                if not ifname:
                    ifname = "br-ex"
            else:
                ifname = self._egress_interface

            parsed_data["interface"] = ifname
            parsed_data["target_ip"] = target_ip
            parsed_data["target_port"] = target_port

            # Capture traffic on the HOST going to the external target
            tcpdump_filter = f"tcp and dst host {target_ip} and dst port {target_port}"
            tcpdump_cmd = (
                f"timeout 30 tcpdump -i {ifname} -n -c 10 '{tcpdump_filter}' 2>&1"
            )

            logger.info(
                f"Running HOST tcpdump for DPU offload verification: {tcpdump_cmd}"
            )
            r = self.run_oc_exec(tcpdump_cmd, may_fail=True)

            parsed_data["tcpdump_output"] = r.out
            parsed_data["tcpdump_success"] = r.success

            self.ts.event_client_finished.wait()

            if not r.success and "0 packets captured" not in r.out:
                if r.returncode != 124:
                    success_result = False
                    msg = f"tcpdump on host failed: {r.err}"
                    logger.error(msg)

            source_ips = self._parse_tcpdump_output(r.out)
            parsed_data["source_ips"] = list(source_ips)
            parsed_data["egress_ip_should_not_appear"] = self._egress_ip

            logger.info(f"Host tcpdump captured source IPs: {source_ips}")

            # Verify that EgressIP is NOT seen on the host
            # If SNAT is happening on the DPU, we should NOT see the EgressIP on the host
            if self._egress_ip in source_ips:
                success_result = False
                msg = (
                    f"DPU offload verification FAILED: EgressIP {self._egress_ip} "
                    f"was seen on the HOST, indicating SNAT is happening on the host, "
                    f"not on the DPU. This may indicate hardware offload is not working."
                )
                logger.error(msg)
            elif not source_ips:
                msg = (
                    "DPU offload verification inconclusive: no traffic captured on host. "
                    "This could indicate traffic is not flowing through the host."
                )
                logger.warning(msg)
            else:
                msg = (
                    f"DPU offload verification PASSED: traffic on host uses "
                    f"source IPs {source_ips}, not EgressIP {self._egress_ip}. "
                    f"SNAT is happening on the DPU as expected."
                )
                logger.info(msg)

            return PluginOutput(
                success=success_result,
                msg=msg,
                plugin_metadata=self.get_plugin_metadata(),
                command=tcpdump_cmd,
                result=parsed_data,
            )

        return TaskOperation(
            log_name=self.log_name,
            thread_action=_thread_action,
        )

    def _aggregate_output_log_success(
        self,
        result: tftbase.AggregatableOutput,
    ) -> None:
        assert isinstance(result, PluginOutput)
        logger.info(f"EgressIP DPU offload verification results: {result.result}")
