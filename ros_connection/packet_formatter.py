from .constants import *

_type_parser = struct.Struct('!B')

def pack(type: MSGTYPE, attr: dict) -> bytes:
    try:
        if type == MSGTYPE.RESPONSE:
            packet = _type_parser.pack(type)
            packet += FORMATTER[MSGTYPE.RESPONSE].pack(
                attr['status'], 
                attr['msg'].encode(encoding='ascii'), 
                attr['timestep'],
                attr['flags'],
                attr['map'].encode(encoding='ascii')
            )
            return packet
        elif type == MSGTYPE.RECV:
            packet = _type_parser.pack(type)
            packet += FORMATTER[MSGTYPE.RECV].pack(
                attr['status'],
                attr['msg'].encode(encoding='ascii'),
                attr['ego_idx'], 
                attr['scans'],
                attr['poses_x'],
                attr['poses_y'],
                attr['poses_theta'],
                attr['linear_vels_x'],
                attr['linear_vels_y'],
                attr['ang_vels_z'],
                attr['collisions'],
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
        elif type == MSGTYPE.SEND:
            steer, speed = FORMATTER[MSGTYPE.SEND].unpack(data[1:])
            attr['steer'] = steer
            attr['speed'] = speed
        else: 
            raise Exception(f"Invalid type {type}")
    except Exception as e:
        raise Exception(f"Exception on unpacking: {e}")
    return type, attr