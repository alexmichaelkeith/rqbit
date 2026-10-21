#[cfg(test)]
pub(crate) mod tests;

use crate::Error;
use std::{ffi::CString, num::NonZeroU32, str::FromStr};

#[derive(Debug, Clone)]
pub struct BindDevice {
    #[allow(unused)]
    index: NonZeroU32,
    #[allow(unused)]
    name: CString,
    #[cfg(windows)]
    ipv4: std::net::Ipv4Addr,
}

impl BindDevice {
    #[cfg(not(windows))]
    pub fn new_from_name(name: &str) -> crate::Result<Self> {
        let name = CString::new(name).map_err(|_| Error::BindDeviceInvalid)?;

        let index = unsafe { libc::if_nametoindex(name.as_ptr()) };
        let index = NonZeroU32::new(index)
            .ok_or_else(|| Error::BindDeviceInvalidError(std::io::Error::last_os_error()))?;
        Ok(Self { index, name })
    }

    #[cfg(windows)]
    pub fn new_from_name(name: &str) -> crate::Result<Self> {
        use network_interface::{Addr, NetworkInterface, NetworkInterfaceConfig};
        use windows_sys::Win32::NetworkManagement::IpHelper::*;
        let wide: Vec<u16> = name.encode_utf16().chain(Some(0)).collect();
        let mut luid = unsafe { std::mem::zeroed() };
        let mut index = 0;
        let result = unsafe { ConvertInterfaceAliasToLuid(wide.as_ptr(), &mut luid) };
        if result != 0 {
            return Err(Error::BindDeviceInvalidError(
                std::io::Error::from_raw_os_error(result as i32),
            ));
        }
        let result = unsafe { ConvertInterfaceLuidToIndex(&luid, &mut index) };
        if result != 0 {
            return Err(Error::BindDeviceInvalidError(
                std::io::Error::from_raw_os_error(result as i32),
            ));
        }
        let index = NonZeroU32::new(index).ok_or(Error::BindDeviceInvalid)?;
        let ipv4 = NetworkInterface::show()
            .map_err(|_| Error::BindDeviceInvalid)?
            .into_iter()
            .find(|interface| interface.index == index.get())
            .and_then(|interface| {
                interface.addr.into_iter().find_map(|addr| match addr {
                    Addr::V4(addr) => Some(addr.ip),
                    _ => None,
                })
            })
            .ok_or(Error::BindDeviceInvalid)?;
        Ok(Self {
            index,
            name: CString::new(name).map_err(|_| Error::BindDeviceInvalid)?,
            ipv4,
        })
    }

    #[cfg(windows)]
    pub fn source_ipv4(&self) -> std::net::Ipv4Addr {
        self.ipv4
    }

    /// Preserve requested ports while selecting the interface's explicit source.
    #[cfg(windows)]
    pub(crate) fn source_address(
        &self,
        addr: std::net::SocketAddr,
    ) -> crate::Result<std::net::SocketAddr> {
        if addr.is_ipv6() {
            return Err(Error::BindDeviceNotSupported);
        }
        if !addr.ip().is_unspecified() && addr.ip() != std::net::IpAddr::V4(self.ipv4) {
            return Err(Error::BindDeviceInvalid);
        }
        Ok((self.ipv4, addr.port()).into())
    }

    pub fn index(&self) -> NonZeroU32 {
        self.index
    }

    pub fn name(&self) -> &str {
        // We constructed from a string so this can't fail
        unsafe { std::str::from_utf8_unchecked(self.name.to_bytes()) }
    }

    #[cfg(target_os = "macos")]
    pub fn bind_sref(&self, sref: &socket2::Socket, is_v6: bool) -> crate::Result<()> {
        if is_v6 {
            sref.bind_device_by_index_v6(Some(self.index))
                .map_err(Error::BindDeviceSetDeviceError)
        } else {
            sref.bind_device_by_index_v4(Some(self.index))
                .map_err(Error::BindDeviceSetDeviceError)
        }
    }

    #[cfg(not(any(target_os = "macos", windows)))]
    pub fn bind_sref(&self, sref: &socket2::Socket, _is_v6: bool) -> crate::Result<()> {
        let name = self.name.as_bytes_with_nul();
        sref.bind_device(Some(name))
            .map_err(Error::BindDeviceSetDeviceError)
    }

    #[cfg(windows)]
    pub fn bind_sref(&self, sref: &socket2::Socket, is_v6: bool) -> crate::Result<()> {
        use std::os::windows::io::AsRawSocket;
        use windows_sys::Win32::Networking::WinSock::*;
        // IPv4-only VPNs must never silently open unprotected IPv6 sockets.
        if is_v6 {
            return Err(Error::BindDeviceNotSupported);
        }
        let index = self.index.get().to_be();
        let result = unsafe {
            setsockopt(
                sref.as_raw_socket() as _,
                IPPROTO_IP,
                IP_UNICAST_IF,
                (&index as *const u32).cast(),
                std::mem::size_of::<u32>() as i32,
            )
        };
        if result == SOCKET_ERROR {
            return Err(Error::BindDeviceSetDeviceError(
                std::io::Error::from_raw_os_error(unsafe { WSAGetLastError() }),
            ));
        }
        Ok(())
    }
}

impl FromStr for BindDevice {
    type Err = crate::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::new_from_name(s)
    }
}
