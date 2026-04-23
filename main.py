import ros_connection

HOST = '127.0.0.1'
PORT = 22200

if __name__ == '__main__':
    server = ros_connection.sim_server()
    server.bind(HOST, PORT)
    server.serve()