import ros_connection
from ros_connection.constants import *
from ros_connection.packet_formatter import *
import numpy as np, gym
import threading, time
import traceback



HOST = '0.0.0.0'
PORT = 22200
GYMENV = 'f110_gym:f110-v0'
MAP_PATH = 'gym/f110_gym/envs/maps/'

RENDERING = True
RECV_TIMEOUT = 1
TIMEOUT = 30

gym_offline_event = threading.Event()
client_offline_event = threading.Event()
terminated_event = threading.Event()
server = ros_connection.sim_server(gym_offline_event, client_offline_event, terminated_event)

def flat_obs(obs: dict):
    ego_idx = obs['ego_idx']
    obs = {k: v[ego_idx] if k != 'ego_idx' else ego_idx for k, v in obs.items()}
    return obs

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
    server.serve(HOST, PORT)

def listen_and_loop():
    init_res = {
        'status': STATUS.READY,
        'msg': ''
    }

    init_req_raw = server.recv(MSGTYPE.INIT_REQUEST, True, 1)
    while init_req_raw == None:
        init_req_raw = server.recv(MSGTYPE.INIT_REQUEST, True, 1)
    
    server.send(init_res, MSGTYPE.INIT_RESPONSE)

    while not client_offline_event.is_set():
        run_gym()

def run_gym():
    while not client_offline_event.is_set():
        start_req = server.recv(MSGTYPE.START_REQUEST, True, 1)
        if start_req != None: 
            break
    if client_offline_event.is_set():
        return

    start_res = {
        'status': STATUS.RUNNING,
        'msg': '',
        'timestep': 0.0,
        'flags': 0,
        'map': ''
    }
    
    start_res['timestep'] = start_req['timestep']
    start_res['flags'] = start_req['flags']
    start_res['map'] = start_req['map']

    try:
        print(f"Preparing F110 gym... (map: {start_req['map']}, timestep: {start_req['timestep']})")
        racecar_env = gym.make(GYMENV, 
                            map=MAP_PATH + start_req['map'], 
                            map_ext='.png', 
                            num_agents=1, 
                            timestep=start_req['timestep']
        )
    except Exception as e:
        print(e)
        client_offline_event.set()

        start_res['status'] = STATUS.FAILURE
        start_res['msg'] = str(e)
        server.send(start_res, MSGTYPE.START_RESPONSE)
        return
    
    gym_offline_event.clear()
    obs, step_reward, done, info = racecar_env.reset(np.array([[0., 0., 0.]]))
    obs = flat_obs(obs)
    lap_time = 0.

    if RENDERING:
        racecar_env.add_render_callback(render_callback)
        racecar_env.render()

    start_res['status'] = STATUS.RUNNING
    start_res['msg'] = 'Ready'
    server.send(start_res, MSGTYPE.START_RESPONSE)

    flag_sync = not (start_req['flags'] & SIMFLAGS.ASYNC)
    flags_str = 'Sync' if flag_sync else 'Async'
    print(f"F110 gym is Ready ({flags_str})")

    while not gym_offline_event.is_set() and not done:
        try:
            act = server.recv(MSGTYPE.SEND, flag_sync, RECV_TIMEOUT)
            if act != None:
                action = [ [act['steer'], act['speed']] ]
            else:
                if flag_sync and lap_time > 0.0:
                    continue
                else:
                    action = [ [0, 0] ]

            obs, step_reward, done, info = racecar_env.step(np.array(action))
            obs = flat_obs(obs)
            lap_time += step_reward

            if RENDERING:
                racecar_env.render()

            send = {
                'status': STATUS.RUNNING if not done else STATUS.DONE,
                'msg': '',
                'elapsed_time': lap_time
            }
            send |= obs
            server.send(send, MSGTYPE.RECV, flag_sync)

        except Exception as e:
            print(f"Error: \n{traceback.format_exc()}")
            send = {
                'status': STATUS.ERROR,
                'msg': str(e),
                'elapsed_time': lap_time
            }
            send |= obs
            server.send(send, MSGTYPE.RECV, flag_sync)
    
    racecar_env.close()
    gym_offline_event.set()

    print("F110 gym closed.")

if __name__ == '__main__':

    gym_offline_event.clear()
    client_offline_event.clear()
    thread_server = threading.Thread(target=ros_server, daemon=True)
    thread_server.start()

    # thread_sim = threading.Thread(target=sim_gym, daemon=True)
    # thread_sim.start()

    try:
        while True:
            listen_and_loop()
    except KeyboardInterrupt as e:
        print(f"Interrupted")
    except Exception as e:
        print(f"Failure: \n{traceback.format_exc()}")
    finally:
        terminated_event.set()
        # thread_server.join()
        # thread_sim.join()
        print("Successfully Terminated")
    