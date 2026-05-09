from .constants import *

_type_parser = struct.Struct('!B')
header_parser = struct.Struct('!I')

def pack(type: MSGTYPE, attr: dict) -> bytes:
    try:
        if type == MSGTYPE.RESPONSE:
            packet = _type_parser.pack(type)
            packet += FORMATTER[MSGTYPE.RESPONSE].pack(
                attr['status'].value, 
                attr['msg'].encode(encoding='ascii'), 
                attr['timestep'],
                attr['flags'],
                attr['map'].encode(encoding='ascii')
            )
            return packet
        elif type == MSGTYPE.RECV:
            packet = _type_parser.pack(type)
            packet += FORMATTER[MSGTYPE.RECV].pack(
                attr['status'].value,
                attr['msg'].encode(encoding='ascii'),
                attr['ego_idx'], 
                *attr['scans'],
                attr['poses_x'],
                attr['poses_y'],
                attr['poses_theta'],
                attr['linear_vels_x'],
                attr['linear_vels_y'],
                attr['ang_vels_z'],
                int(attr['collisions']),
                attr['elapsed_time']
            )
            return packet
        else: 
            raise Exception(f"Invalid type {type}")
    except Exception as e:
        raise Exception(f"Exception on packing: {e}")

def unpack(data: bytes) -> tuple[MSGTYPE, dict]:
    try:
        type = _type_parser.unpack(data[0:1])[0]
        attr = {}
        if type == MSGTYPE.REQUEST:
            timestep, flags, map = FORMATTER[MSGTYPE.REQUEST].unpack(data[1:])
            attr['timestep'] = timestep
            attr['flags'] = flags
            attr['map'] = map.decode(encoding='ascii').strip('\x00')
        elif type == MSGTYPE.RESPONSE:
            status, msg, timestep, flags, map = FORMATTER[MSGTYPE.RESPONSE].unpack(data[1:])
            attr['status'] = status
            attr['msg'] = msg.decode(encoding='ascii').strip('\x00')
            attr['timestep'] = timestep
            attr['flags'] = flags
            attr['map'] = map.decode(encoding='ascii').strip('\x00')
        elif type == MSGTYPE.SEND:
            steer, speed = FORMATTER[MSGTYPE.SEND].unpack(data[1:])
            attr['steer'] = steer
            attr['speed'] = speed
        elif type == MSGTYPE.RECV:
            parsed = FORMATTER[MSGTYPE.RECV].unpack(data[1:])
            status, msg, egoidx = parsed[0], parsed[1], parsed[2]
            scans, poses, vels = parsed[3:1083], parsed[1083:1086], parsed[1086:1089]
            iscols, elapsed_time = parsed[1089], parsed[1090]
            obs = {}
            obs['ego_idx'], obs['scans'] = egoidx, scans
            obs['poses_x'], obs['poses_y'], obs['poses_theta'] = poses[0], poses[1], poses[2]
            obs['linear_vels_x'], obs['linear_vels_y'], obs['ang_vels_z'] = vels[0], vels[1], vels[2]
            obs['collisions'] = iscols
            attr['obs'] = obs
            attr['status'] = status
            attr['msg'] = msg.decode(encoding='ascii').strip('\x00')
            attr['elapsed_time'] = elapsed_time
        else: 
            raise Exception(f"Invalid type {type}")
    except Exception as e:
        raise Exception(f"Exception on unpacking: {e}")
    return type, attr