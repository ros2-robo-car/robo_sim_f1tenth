import ros_connection
import gym
import numpy as np
import threading, time

HOST = '127.0.0.1'
PORT = 22200
GYMENV = 'f110_gym:f110-v0'
GYMMAP = 'examples/example_map'

terminated_event = threading.Event()

send_data = ros_connection.lock_list(ros_connection.LIDAR_COUNT)    # LiDAR Data
recv_data = ros_connection.lock_list(2) # Control Data

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
    server = ros_connection.sim_server()
    server.bind(HOST, PORT)
    server.getLiDARData = send_data.get
    server.onReceive = lambda steer, speed: recv_data.set([steer, speed])
    print("Sim Server Ready")

    server.serve(close_event=terminated_event)
    print("Sim Server Close")

def sim_gym():
    racecar_env = gym.make(GYMENV, 
                        map=GYMMAP, 
                        map_ext='.png', 
                        num_agents=1, 
                        timestep=0.0125
    )
    racecar_env.add_render_callback(render_callback)

    obs, step_reward, done, info = racecar_env.reset(np.array([[0., 0., 0.]]))
    racecar_env.render()
    lap_time = 0.
    print("Simulation Ready")

    while not terminated_event.is_set():
        actions = np.array([recv_data.get()])
        if done:
            obs, step_reward, done, info = racecar_env.reset(np.array([[0., 0., 0.]]))
            recv_data.set([0., 0.])
            lap_time = 0.
        else:
            obs, step_reward, done, info = racecar_env.step(actions)
            lap_time += step_reward
        send_data.set(obs['scans'][0])
        racecar_env.render()

    print("Simulation terminated")

if __name__ == '__main__':

    thread_server = threading.Thread(target=ros_server, daemon=True)
    thread_server.start()

    # thread_sim = threading.Thread(target=sim_gym, daemon=True)
    # thread_sim.start()

    try:
        sim_gym()
    except KeyboardInterrupt as e:
        print(f"Interrupted")
    except Exception as e:
        print(f"Exception: {e}")
    finally:
        terminated_event.set()
        thread_server.join()
        # thread_sim.join()
        print("Successfully Terminated")
    