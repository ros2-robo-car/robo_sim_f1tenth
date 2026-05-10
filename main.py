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

RENDERING = False
RECV_TIMEOUT = 1
TIMEOUT = 30

gym_online_event = threading.Event()
client_online_event = threading.Event()
terminated_event = threading.Event()
server = ros_connection.sim_server(gym_online_event, client_online_event, terminated_event)

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

    init_req_raw = server.recv(True, 1)
    while init_req_raw == None:
        init_req_raw = server.recv(True, 1)

    msgtype = get_type(init_req_raw)
    if msgtype != MSGTYPE.INIT_REQUEST:
        init_res = {
            'status': STATUS.ERROR,
            'msg': f'Expected INIT_REQUESET, Received {msgtype.name} ({msgtype})'
        }
        print(init_res['msg'])
        init_res_raw = pack(MSGTYPE.INIT_RESPONSE, init_res)
        server.send(init_res_raw)
        return
    
    init_res_raw = pack(MSGTYPE.INIT_RESPONSE, init_res)
    server.send(init_res_raw)

    while client_online_event.is_set():
        run_gym()

def run_gym():
    recv = server.recv(True, 1)
    while recv == None and client_online_event.is_set():
        recv = server.recv(True, 1)

    start_res = {
        'status': STATUS.RUNNING,
        'msg': '',
        'timestep': 0.0,
        'flags': 0,
        'map': ''
    }
    try:
        msgtype, start_req = unpack(recv)
        if msgtype != MSGTYPE.START_REQUEST:
            raise Exception(f'Expected START_REQUESET, Received {msgtype.name} ({msgtype})')
    except Exception as e:
        start_res['status'] = STATUS.ERROR
        start_res['msg'] = str(e)
        start_res_raw = pack(MSGTYPE.START_RESPONSE, start_res)
        server.send(start_res_raw)
        return
    
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
        start_res['status'] = STATUS.FAILURE
        start_res['msg'] = str(e)
        start_res_raw = pack(MSGTYPE.START_RESPONSE, start_res)
        client_online_event.clear()
        server.send(start_res_raw)
        return
    
    gym_online_event.set()
    obs, step_reward, done, info = racecar_env.reset(np.array([[0., 0., 0.]]))
    obs = flat_obs(obs)
    lap_time = 0.

    if RENDERING:
        racecar_env.add_render_callback(render_callback)
        racecar_env.render()

    start_res['status'] = STATUS.RUNNING
    start_res['msg'] = 'Ready'
    start_res_raw = pack(MSGTYPE.START_RESPONSE, start_res)
    server.send(start_res_raw)

    flag_sync = not (start_req['flags'] & SIMFLAGS.ASYNC)
    flags_str = 'Sync' if flag_sync else 'Async'
    print(f"F110 gym is Ready ({flags_str})")

    while gym_online_event.is_set() and not done:
        try:
            recv = server.recv(flag_sync, RECV_TIMEOUT)
            if recv != None:
                msgtype, act = unpack(recv)
                if msgtype != MSGTYPE.SEND:
                    raise Exception(f'Expected SEND, Received {msgtype.name} ({msgtype})')
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
            send_raw = pack(MSGTYPE.RECV, send)
            server.send(send_raw, flag_sync)

        except Exception as e:
            print(f"Error: \n{traceback.format_exc()}")
            send = {
                'status': STATUS.ERROR,
                'msg': str(e),
                'elapsed_time': lap_time
            }
            send |= obs
            send_raw = pack(MSGTYPE.RECV, send)
            server.send(send_raw, flag_sync)
    
    racecar_env.close()
    gym_online_event.clear()

    print("F110 gym closed.")

if __name__ == '__main__':

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
    