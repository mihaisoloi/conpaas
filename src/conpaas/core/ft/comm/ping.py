'''
# -- ConPaaS fault tolerance --
#
# Definition of the ping protocol used by the active failure
# detection. This class contains the encoding and decoding
# of the messages sent to test whether a role is still
# running.
#
# @author Cristina Basescu (cristina.basescu@gmail.com)
'''

import struct

MSG_PING3 = 1
MSG_PING4 = 2
MSG_PING_REQ = 3
MSG_RESPONSE = 4

class PingError(Exception):
    pass

class PingProtocol():
    def __init__(self):
        pass

    @staticmethod
    def __len_field_size():
        return struct.Struct('I').size

    @staticmethod
    def __type_field_size():
        return struct.Struct('B').size

    '''
    Encodes a message to be sent on a socket
    Msg format: length - 4 bytes, message type - 1 byte, msg

    @param msg, type string - message to send
    @msg_type - MSG_PING3, MSG_PING4, MSG_PING_REQ

    @raise PingError - if the message type is incorrect

    @return, type bytes - the encoded message
    '''
    @staticmethod
    def encode_msg(msg_type, msg):
        if(msg_type != MSG_PING3 and msg_type != MSG_PING4 and msg_type != MSG_PING_REQ and msg_type != MSG_RESPONSE):
            raise PingError("Incorrect message type")

        length = len(msg)
        msg_bytes = bytearray(PingProtocol.__len_field_size() + PingProtocol.__type_field_size() + length)
        fmt = "!IB"+str(length)+"s"
        struct.pack_into(fmt, msg_bytes, 0, length, msg_type, msg.encode())
        return msg_bytes

    '''
    Decodes the length of the message

    @param len_bytes, type bytes - length of the message in bytes

    @return, type int - the length of the message as integer
    '''
    @staticmethod
    def __decode_len(len_bytes):
        fields = struct.unpack_from("!I", len_bytes)
        return fields[0]

    '''
    Decodes the message received from the socket

    @param msg_bytes, type bytes - the message to decode in bytes

    @return, type tuple<byte, str> - the message type and the message contents
    '''
    @staticmethod
    def decode_msg(raw_bytes):
        len_bytes = raw_bytes[0 : PingProtocol.__len_field_size()]
        msg_len = PingProtocol.__decode_len(len_bytes)

        msg_bytes = raw_bytes[PingProtocol.__len_field_size() : msg_len + PingProtocol.__type_field_size() + PingProtocol.__len_field_size()]
        fmt = "!B"+str(msg_len)+"s"
        fields = struct.unpack(fmt, msg_bytes)
        return (fields[0], fields[1].decode())
