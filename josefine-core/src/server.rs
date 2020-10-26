use iron::prelude::*;
use iron::status;
use router::Router;

pub struct Server {
    address: String,
    broker: Broker,
    server: Iron<Router>,
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
    pub fn new(address: String, broker: Broker) -> Server {
        let mut router = Router::new();
        router.get("/", Server::get, "get");
        router.post("/", Server::set, "set");
        router.delete("/", Server::delete, "delete");

        let server = Iron::new(router);

        Server {
            address,
            broker,
            server,
        }
    }

    fn get(_req: &mut Request) -> IronResult<Response> {
        Ok(Response::with((status::Ok, "get")))
    }

    fn set(_req: &mut Request) -> IronResult<Response> {
        Ok(Response::with((status::Ok, "set")))
    }

    fn delete(_req: &mut Request) -> IronResult<Response> {
        Ok(Response::with((status::Ok, "delete")))
    }

    pub fn start(self) {
        self.server.http(self.address).unwrap();
    }
}
