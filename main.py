import ros_connection
from ros_connection.constants import *
from ros_connection.packet_formatter import *
import numpy as np, gym
import traceback
import threading


HOST = '0.0.0.0'
PORT = 22200
GYMENV = 'f110_gym:f110-v0'
MAP_PATH = 'gym/f110_gym/envs/maps/'

RENDERING = False
RECV_TIMEOUT = 1

gym_online_event = threading.Event()
terminated_event = threading.Event()
server = ros_connection.sim_server(gym_online_event, terminated_event)


res = {
    'status': STATUS.READY,
    'msg': '',
    'timestep': 0.0,
    'flags': 0,
    'map': ''
}

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

def listen_and_loop_gym():
    recv = None
    while recv == None:
        recv = server.recv(True, 1)

    msgtype, req = unpack(recv)
    if msgtype != MSGTYPE.REQUEST:
        print(f'Expected REQUESET, Received {msgtype.name} ({msgtype})')
        return

    loop_gym(req)
    print("F110 gym closed.")

def loop_gym(req):
    res = {}
    res['timestep'] = req['timestep']
    res['flags'] = req['flags']
    res['map'] = req['map']

    try:
        print(f"Preparing F110 gym... (map: {req['map']}, timestep: {req['timestep']})")
        racecar_env = gym.make(GYMENV, 
                            map=MAP_PATH + req['map'], 
                            map_ext='.png', 
                            num_agents=1, 
                            timestep=req['timestep']
        )
    except Exception as e:
        print(e)
        res['status'] = STATUS.FAILURE
        res['msg'] = str(e)
        response = pack(MSGTYPE.RESPONSE, res)
        server.send(response)
        return
    
    gym_online_event.set()
    obs, step_reward, done, info = racecar_env.reset(np.array([[0., 0., 0.]]))
    obs = flat_obs(obs)
    lap_time = 0.

    if RENDERING:
        racecar_env.add_render_callback(render_callback)
        racecar_env.render()

    res['status'] = STATUS.READY
    res['msg'] = 'Ready'
    response = pack(MSGTYPE.RESPONSE, res)
    server.send(response)

    flag_sync = not (req['flags'] & SIMFLAGS.ASYNC)
    flags_str = 'Sync' if flag_sync else 'Async'
    print(f"F110 gym is Ready ({flags_str})")

    while gym_online_event.is_set() and not done:
        try:
            recv = server.recv(flag_sync, RECV_TIMEOUT)
            if recv != None:
                msgtype, act = unpack(recv)
                action = [ [act['steer'], act['speed']] ]
                if msgtype != MSGTYPE.SEND:
                    print(f'Expected SEND, Received {msgtype.name} ({msgtype})')
                    continue
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

            response = {
                'status': STATUS.RUNNING if not done else STATUS.DONE,
                'msg': '',
                'elapsed_time': lap_time
            }
            response |= obs
            res = pack(MSGTYPE.RECV, response)
            server.send(res, flag_sync)

        except Exception as e:
            print(f"Error: \n{traceback.format_exc()}")
            response = {
                'status': STATUS.ERROR,
                'msg': str(e),
                'elapsed_time': lap_time
            }
            response |= obs
            res = pack(MSGTYPE.RECV, response)
            server.send(res, flag_sync)
    
    racecar_env.close()
    gym_online_event.clear()

if __name__ == '__main__':

    thread_server = threading.Thread(target=ros_server, daemon=True)
    thread_server.start()

    # thread_sim = threading.Thread(target=sim_gym, daemon=True)
    # thread_sim.start()

    try:
        while True:
            server.flush()
            listen_and_loop_gym()
    except KeyboardInterrupt as e:
        print(f"Interrupted")
    except Exception as e:
        print(f"Failure: \n{traceback.format_exc()}")
    finally:
        terminated_event.set()
        # thread_server.join()
        # thread_sim.join()
        print("Successfully Terminated")
    