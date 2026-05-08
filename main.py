import ros_connection
from ros_connection.packet_formatter import *
import gym
import numpy as np
import threading

import time # for test sleep

HOST = '0.0.0.0'
PORT = 22200
GYMENV = 'f110_gym:f110-v0'

server = ros_connection.sim_server()
terminate_event = threading.Event()

def render_callback(env_renderer):
    e = env_renderer
    x = e.cars[0].vertices[::2]
    y = e.cars[0].vertices[1::2]
    top, bottom, left, right = max(y), min(y), min(x), max(x)
    e.score_label.x = left
    e.score_label.y = top - 700
    e.left = left - 800
    e.right = right + 800
    e.top = top + 800
    e.bottom = bottom - 800

def ros_server():
    server.serve(HOST, PORT, terminate_event)

def sim_gym():
    err_response = {
        'status': STATUS.ERROR,
        'msg': '',
        'timestep': 0.0,
        'flags': 0,
        'map': ''
    }

    t, req = unpack(server.recv())
    if t != MSGTYPE.REQUEST:
        err_response['msg'] = 'Sim server expected REQUEST packet.'
        print()
        msg = pack(MSGTYPE.RESPONSE, err_response)
        server.send(msg)
        return
    
    try:
        racecar_env = gym.make(GYMENV, 
                            map=req['map'], 
                            map_ext='.png', 
                            num_agents=1, 
                            timestep=req['timestep']
        )
    except Exception as e:
        err_response['msg'] = str(e)
        msg = pack(MSGTYPE.RESPONSE, err_response)
        server.send(msg)
        return
    
    # racecar_env.add_render_callback(render_callback)

    obs, step_reward, done, info = racecar_env.reset(np.array([[0., 0., 0.]]))
    # racecar_env.render()
    lap_time = 0.
    print("Simulation Ready")

    while done:
        actions = np.array([recv_data.get()])
        obs, step_reward, done, info = racecar_env.step(actions)
        lap_time += step_reward
        send_data.set(obs['scans'][0])
        # racecar_env.render()

    print("Simulation terminated")

if __name__ == '__main__':

    thread_server = threading.Thread(target=ros_server, daemon=True)
    thread_server.start()

    # thread_sim = threading.Thread(target=sim_gym, daemon=True)
    # thread_sim.start()

    try:
        # sim_gym()
        while True:
            time.sleep(1)
    except KeyboardInterrupt as e:
        print(f"Interrupted")
    except Exception as e:
        print(f"Exception: {e}")
    finally:
        terminate_event.set()
        # thread_server.join()
        # thread_sim.join()
        print("Successfully Terminated")
    