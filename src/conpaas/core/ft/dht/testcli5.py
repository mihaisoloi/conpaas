from conpaas.core.ft.dht import server

peer = server.Server(('10.100.0.125', 9995), 101, ('10.100.0.125', 9990))
peer.serve_forever()
