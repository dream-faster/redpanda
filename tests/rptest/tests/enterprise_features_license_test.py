import json
from enum import IntEnum

from ducktape.mark import matrix
from ducktape.utils.util import wait_until

from rptest.clients.rpk import RpkTool
from rptest.services.admin import (
    Admin,
    EnterpriseLicenseStatus,
    RolesList,
)
from rptest.services.cluster import cluster
from rptest.services.redpanda import (
    SecurityConfig,
)
from rptest.tests.redpanda_test import RedpandaTest
from rptest.utils.mode_checks import skip_fips_mode


class EnterpriseFeaturesTestBase(RedpandaTest):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.admin = Admin(self.redpanda)
        self.installer = self.redpanda._installer

    def setUp(self):
        return


class Feature(IntEnum):
    audit_logging = 0
    partition_auto_balancing_continuous = 1
    core_balancing_continuous = 2
    rbac = 3
    fips = 4
    leadership_pinning = 5
    topic_deletion_disabled = 6


enterprise_features = set(Feature)
autoenable_features = set(
    [Feature.partition_auto_balancing_continuous, Feature.core_balancing_continuous]
)

FEATURE_DEPENDENT_CONFIG = {
    Feature.audit_logging: "audit_enabled",
    Feature.partition_auto_balancing_continuous: "partition_autobalancing_mode",
    Feature.core_balancing_continuous: "core_balancing_continuous",
    Feature.leadership_pinning: "default_leaders_preference",
    Feature.topic_deletion_disabled: "delete_topic_enable",
}

SKIP_FEATURES = [
    Feature.fips,  # NOTE(oren): omit because it's too much of a pain for CDT
]


class EnterpriseFeaturesTest(EnterpriseFeaturesTestBase):
    def __init__(self, *args, **kwargs):
        super().__init__(
            *args,
            num_brokers=3,
            **kwargs,
        )

        self.su, self.pw, self.mech = self.redpanda.SUPERUSER_CREDENTIALS

        self.security = SecurityConfig()
        self.security.enable_sasl = True
        self.kafka_enable_authorization = True
        self.endpoint_authn_method = "sasl"
        self.redpanda.set_security_settings(self.security)

    def check_feature(
        self,
        feature: Feature,
        enabled: bool,
        license_valid: bool,
    ) -> dict[Feature, bool]:
        rsp = self.admin.get_enterprise_features().json()
        feature_statuses: dict[Feature, bool] = {
            Feature[f["name"]]: f["enabled"] for f in rsp.get("features", [])
        }
        ELS = EnterpriseLicenseStatus

        assert feature_statuses.get(feature) == enabled, (
            f"Expected {feature.name} {enabled=}"
        )
        assert (ELS(rsp.get("license_status")) == ELS.valid) is license_valid, (
            f"Expected {license_valid=} (got {rsp.get('license_status')})"
        )

        return feature_statuses

    @skip_fips_mode
    @cluster(num_nodes=3)
    @matrix(
        with_license=[
            True,
            False,
        ]
    )
    def test_get_enterprise(self, with_license):
        if not with_license:
            self.redpanda.set_environment(
                dict(__REDPANDA_DISABLE_BUILTIN_TRIAL_LICENSE="1")
            )

        self.redpanda.start()

        statuses = self.check_feature(
            Feature.audit_logging, enabled=False, license_valid=with_license
        )

        expected_statuses = {
            f: {"status": statuses[f], "expected": f in autoenable_features}
            for f in enterprise_features
        }

        assert all(
            [s["status"] == s["expected"] for _, s in expected_statuses.items()]
        ), f"Unexpected feature status: {json.dumps(expected_statuses, indent=1)}"

    def try_enable_feature(self, feature):
        if feature == Feature.audit_logging:
            self.redpanda.set_cluster_config(
                {
                    "audit_enabled": True,
                    "audit_use_rpc": True,
                },
                expect_restart=True,
            )
        elif feature == Feature.partition_auto_balancing_continuous:
            self.redpanda.set_cluster_config(
                {"partition_autobalancing_mode": "continuous"}
            )
        elif feature == Feature.core_balancing_continuous:
            self.redpanda.set_cluster_config({"core_balancing_continuous": "true"})
        elif feature == Feature.rbac:
            # NOTE(oren): make sure the role has propagated to every node since we don't know
            # where the get_enterprise request will go
            def has_role(r: str):
                return all(
                    len(
                        RolesList.from_response(
                            self.admin.list_roles(filter=r, node=n)
                        ).roles
                    )
                    > 0
                    for n in self.redpanda.nodes
                )

            self.admin.create_role("dummy")
            wait_until(lambda: has_role("dummy"), timeout_sec=30, backoff_sec=1)
        elif feature == Feature.fips:
            self.redpanda.rolling_restart_nodes(
                self.redpanda.nodes,
                override_cfg_params={
                    "fips_mode": "permissive",
                    "openssl_config_file": self.redpanda.get_openssl_config_file_path(),
                    "openssl_module_directory": self.redpanda.get_openssl_modules_directory(),
                },
            )
        elif feature == Feature.leadership_pinning:
            self.redpanda.set_cluster_config(
                {"default_leaders_preference": "racks:rack1"}, expect_restart=True
            )
            RpkTool(self.redpanda).create_topic(
                "foo",
                partitions=1,
                replicas=1,
                config={"redpanda.leaders.preference": "racks:rack1"},
            )
        elif feature == Feature.topic_deletion_disabled:
            self.redpanda.set_cluster_config({"delete_topic_enable": "false"})
        else:
            assert False, f"Unexpected feature={feature}"

    @skip_fips_mode
    @cluster(num_nodes=3)
    @matrix(
        feature=[f for f in Feature if f not in SKIP_FEATURES],
        install_license=[
            True,
            False,
        ],
        disable_trial=[
            False,
            True,
        ],
    )
    def test_enable_features(self, feature, install_license, disable_trial):
        if disable_trial:
            self.redpanda.set_environment(
                dict(__REDPANDA_DISABLE_BUILTIN_TRIAL_LICENSE="1")
            )

        self.redpanda.start()

        if install_license:
            self.redpanda.install_license()

        has_license = not disable_trial or install_license

        # This fork does not enforce licenses: enterprise features can be
        # enabled with or without one.
        self.try_enable_feature(feature)

        self.logger.debug(f"Check that {feature.name} has the expected state")

        statuses = self.check_feature(
            feature,
            enabled=True,
            license_valid=has_license,
        )

        self.logger.debug(
            "Everything else should be in the default state regardless of license status"
        )

        other_features = [f for f in enterprise_features if f != feature]
        other_statuses = {
            f: {"status": statuses[f], "expected": f in autoenable_features}
            for f in other_features
        }

        assert all([s["status"] == s["expected"] for _, s in other_statuses.items()]), (
            f"Unexpected feature status: {json.dumps(other_statuses, indent=1)}"
        )

    @skip_fips_mode
    @cluster(num_nodes=3)
    def test_license_violation(self):
        self.logger.debug(
            "Suppress the trial license before starting redpanda the first time"
        )
        self.redpanda.set_environment(
            dict(__REDPANDA_DISABLE_BUILTIN_TRIAL_LICENSE="1")
        )

        self.logger.debug("Switch on any old enterprise feature and start")
        feature = Feature.core_balancing_continuous
        self.redpanda._extra_rp_conf.update({FEATURE_DEPENDENT_CONFIG[feature]: True})
        self.redpanda.start()

        self.logger.debug("Confirm license violation per GET /features/enterprise")
        self.check_feature(feature, enabled=True, license_valid=False)

        self.redpanda.set_cluster_config({FEATURE_DEPENDENT_CONFIG[feature]: "false"})

        self.logger.debug(
            "We should be able to revert to a compliant setting without issue"
        )

        self.check_feature(feature, enabled=False, license_valid=False)
