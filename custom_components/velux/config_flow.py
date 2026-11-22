"""Config flow for Velux integration."""

import asyncio
from typing import Any

from pyvlx import PyVLX, PyVLXException
from pyvlx.discovery import VeluxDiscovery, VeluxHost
import voluptuous as vol

from homeassistant.components import zeroconf
from homeassistant.config_entries import ConfigEntryState, ConfigFlow, ConfigFlowResult
from homeassistant.const import CONF_HOST, CONF_MAC, CONF_NAME, CONF_PASSWORD
import homeassistant.helpers.config_validation as cv
from homeassistant.data_entry_flow import AbortFlow
from homeassistant.helpers.device_registry import format_mac
from homeassistant.helpers.selector import (
    SelectSelector,
    SelectSelectorConfig,
    SelectSelectorMode,
)
from homeassistant.helpers.service_info.dhcp import DhcpServiceInfo
from homeassistant.helpers.service_info.zeroconf import ZeroconfServiceInfo


from .const import DOMAIN, LOGGER

USER_SCHEMA = vol.Schema(
    {
        vol.Required(CONF_HOST): cv.string,
        vol.Required(CONF_PASSWORD): cv.string,
    }
)


async def _check_connection(host: str, password: str) -> dict[str, Any]:
    """Check if we can connect to the Velux bridge."""
    pyvlx = PyVLX(host=host, password=password)
    try:
        await pyvlx.connect()
    except (PyVLXException, ConnectionError) as err:
        LOGGER.debug("Cannot connect: %s", err)
        if "Login to KLF 200 failed" in str(err):
            return {"base": "invalid_auth"}
        return {"base": "cannot_connect"}
    except Exception as err:  # noqa: BLE001
        LOGGER.exception("Unexpected exception: %s", err)
        return {"base": "unknown"}
    finally:
        await _close_test_connection(pyvlx)

    return {}


async def _close_test_connection(pyvlx: PyVLX) -> None:
    """Close the test connection cleanly, without rebooting the KLF 200.

    pyvlx.disconnect() reboots the gateway, so the entry setup that follows a
    successful check would hit a rebooting KLF 200. After a failed login the
    TLS session was left open and blocked the next attempt. Both are avoided
    here: stop the heartbeat, switch off the house status monitor if it is
    on, and always close the socket.
    """
    try:
        await pyvlx.heartbeat.stop()
        if pyvlx.connection.connected and pyvlx.klf200.house_status_monitor_enabled:
            await pyvlx.klf200.house_status_monitor_disable(pyvlx=pyvlx, timeout=5)
    except (PyVLXException, OSError) as err:
        LOGGER.debug("Cleanup of test connection failed: %s", err)
    finally:
        if pyvlx.connection.transport is not None or pyvlx.connection.connected:
            pyvlx.connection.disconnect()
        if pyvlx.connection.tasks:
            await asyncio.gather(*pyvlx.connection.tasks, return_exceptions=True)


def _normalize_name(hostname: str) -> str:
    """Return the KLF 200 name in one consistent form.

    The KLF 200 reports its hostname via zeroconf with a "LAN_" prefix
    (VELUX_KLF_LAN_345B.local.), via DHCP without it (velux_klf_345b).
    Both have to result in the same unique_id, otherwise an already
    configured gateway is offered again as newly discovered.
    """
    name = hostname.upper()
    for suffix in ("._HTTP._TCP.LOCAL.", ".LOCAL.", ".LOCAL"):
        if name.endswith(suffix):
            name = name[: -len(suffix)]
    return name.replace("LAN_", "")


class VeluxConfigFlow(ConfigFlow, domain=DOMAIN):  # type: ignore
    """Handle a config flow for velux."""

    VERSION = 1
    MINOR_VERSION = 2

    def __init__(self) -> None:
        """Initialize the config flow."""
        self.discovery_data: dict[str, Any] = {}
        self.hosts: list[VeluxHost] = []

    async def async_step_user(
        self, user_input: dict[str, str] | None = None
    ) -> ConfigFlowResult:
        """Handle the initial step."""
        errors: dict[str, str] = {}

        if user_input is not None:
            self._async_abort_entries_match({CONF_HOST: user_input[CONF_HOST]})
            if self.hosts:
                for host in self.hosts:
                    if user_input[CONF_HOST] == host.ip_address:
                        await self.async_set_unique_id(_normalize_name(host.hostname))
                        self._abort_if_unique_id_configured(
                            updates={CONF_HOST: host.ip_address}
                        )
            errors = await _check_connection(
                user_input[CONF_HOST], user_input[CONF_PASSWORD]
            )
            if not errors:
                return self.async_create_entry(
                    title=user_input[CONF_HOST],
                    data=user_input,
                )

        if not self.hosts:
            aiozc = await zeroconf.async_get_async_instance(self.hass)
            vd: VeluxDiscovery = VeluxDiscovery(zeroconf=aiozc)
            if await vd.async_discover_hosts(expected_hosts=1):
                self.hosts = vd.hosts  # type: ignore[assignment]

        if self.hosts:
            data_schema = vol.Schema(
                {
                    vol.Required(CONF_HOST): SelectSelector(
                        SelectSelectorConfig(
                            options=[host.ip_address for host in self.hosts],
                            custom_value=True,
                            mode=SelectSelectorMode.DROPDOWN,
                        )
                    ),
                    vol.Required(CONF_PASSWORD): cv.string,
                }
            )
        else:
            data_schema = self.add_suggested_values_to_schema(USER_SCHEMA, user_input)

        return self.async_show_form(
            step_id="user",
            data_schema=data_schema,
            errors=errors,
        )

    def _abort_if_known_gateway(self, name: str, host: str) -> None:
        """Abort if the gateway is already configured under any name variant.

        Older versions stored the unique_id with or without the "LAN_" prefix
        depending on how the entry was created. The stored unique_id is not
        rewritten, because entity and device ids are derived from it.
        """
        for entry in self.hass.config_entries.async_entries(
            DOMAIN, include_ignore=True
        ):
            if entry.unique_id and _normalize_name(entry.unique_id) == name:
                if entry.data.get(CONF_HOST) not in (None, host):
                    self.hass.config_entries.async_update_entry(
                        entry=entry, data={**entry.data, CONF_HOST: host}
                    )
                raise AbortFlow("already_configured")

    async def async_step_unignore(self, user_input: dict[str, Any]) -> ConfigFlowResult:
        """Rediscover a previously ignored discover."""
        unique_id = user_input["unique_id"]
        await self.async_set_unique_id(unique_id)
        return await self.async_step_user()

    async def async_step_zeroconf(
        self, discovery_info: ZeroconfServiceInfo
    ) -> ConfigFlowResult:
        """Handle discovery by zeroconf."""
        self.discovery_data[CONF_NAME] = _normalize_name(discovery_info.hostname)
        self.discovery_data[CONF_HOST] = discovery_info.host
        self._abort_if_known_gateway(
            self.discovery_data[CONF_NAME], self.discovery_data[CONF_HOST]
        )
        await self.async_set_unique_id(self.discovery_data[CONF_NAME])
        self._abort_if_unique_id_configured(
            updates={CONF_HOST: self.discovery_data[CONF_HOST]}
        )

        # Abort if config_entry already exists without unigue_id configured.
        for entry in self.hass.config_entries.async_entries(DOMAIN):
            if entry.data[CONF_HOST] == self.discovery_data[CONF_HOST] and entry.unique_id is None:
                self.hass.config_entries.async_update_entry(
                    entry=entry, unique_id=self.discovery_data[CONF_NAME]
                )
                return self.async_abort(reason="already_configured")
        self._async_abort_entries_match(
            {CONF_HOST: self.discovery_data[CONF_HOST]}
        )
        return await self.async_step_discovery_confirm()

    async def async_step_dhcp(
        self, discovery_info: DhcpServiceInfo
    ) -> ConfigFlowResult:
        """Handle discovery by DHCP."""
        # The hostname ends with the last 4 digits of the device MAC address.
        self.discovery_data[CONF_HOST] = discovery_info.ip
        self.discovery_data[CONF_MAC] = format_mac(discovery_info.macaddress)
        self.discovery_data[CONF_NAME] = _normalize_name(discovery_info.hostname)
        LOGGER.debug(f"Discovered by DHCP with Info: {discovery_info}")
        self._abort_if_known_gateway(
            self.discovery_data[CONF_NAME], self.discovery_data[CONF_HOST]
        )
        await self.async_set_unique_id(self.discovery_data[CONF_NAME])
        self._abort_if_unique_id_configured(
            updates={
                CONF_HOST: self.discovery_data[CONF_HOST],
                CONF_MAC: self.discovery_data[CONF_MAC]
            }
        )

        # Abort if config_entry already exists without unigue_id configured.
        for entry in self.hass.config_entries.async_entries(DOMAIN):
            if (
                entry.data[CONF_HOST] == self.discovery_data[CONF_HOST]
                and entry.unique_id is None
                and entry.state is ConfigEntryState.LOADED
            ):
                self.hass.config_entries.async_update_entry(
                    entry=entry,
                    unique_id=self.discovery_data[CONF_NAME],
                    data={**entry.data, **self.discovery_data},
                )
                return self.async_abort(reason="already_configured")
        self._async_abort_entries_match(
            {CONF_HOST: self.discovery_data[CONF_HOST]}
        )
        return await self.async_step_discovery_confirm()

    async def async_step_discovery_confirm(
        self, user_input: dict[str, Any] | None = None
    ) -> ConfigFlowResult:
        """Prepare configuration for a discovered Velux device."""
        errors: dict[str, str] = {}
        if user_input is not None:
            errors = await _check_connection(
                self.discovery_data[CONF_HOST], user_input[CONF_PASSWORD]
            )
            if not errors:
                return self.async_create_entry(
                    title=self.discovery_data[CONF_NAME],
                    data={**self.discovery_data, **user_input},
                )

        return self.async_show_form(
            step_id="discovery_confirm",
            data_schema=vol.Schema(
                {
                    vol.Required(CONF_PASSWORD): cv.string,
                }
            ),
            errors=errors,
            description_placeholders={
                "name": self.discovery_data[CONF_NAME],
                "host": self.discovery_data[CONF_HOST],
            },
        )
