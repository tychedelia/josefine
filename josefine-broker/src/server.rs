use std::net::SocketAddr;

use josefine_core::error::Result;
use tokio::net::TcpListener;
use futures::FutureExt;

use crate::tcp;

pub struct Server {
    address: String,
}

pub struct Broker {
    id: u64,
    host: String,
    port: String,
}

impl Broker {
    pub fn new(id: u64, host: String, port: String) -> Broker {
        Broker { id, host, port }
    }
}

impl Server {
    pub fn new(address: String) -> Self {
        Server {
            address,
        }
    }

    pub async fn run(
        self
    ) -> Result<()> {
        let socket_addr: SocketAddr = self.address.parse()?;
        let listener = TcpListener::bind(socket_addr).await?;
        let (in_tx, _out_tx) = tokio::sync::mpsc::unbounded_channel::<String>();
        let (task, tcp_receiver) = tcp::receive_task(josefine_core::logger::get_root_logger().new(o!()), listener, in_tx).remote_handle();
        tokio::spawn(task);

        let _ = tokio::try_join!(tcp_receiver)?;
        Ok(())
    }
}
