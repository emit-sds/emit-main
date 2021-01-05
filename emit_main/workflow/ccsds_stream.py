"""
This code is borrowed from Michael Joyce to assist in reading CCSDS packets.

Author: Michael Joyce, Michael.J.Joyce@jpl.nasa.gov
"""

# TODO: Review this code and update for EMIT


class CCSDSPacket():
    """ CCSDS Space Packet Object

    Provides an abstraction of a CCSDS Space Packet to simplify handling CCSDS
    packet data. The CCSDS Packet object will automatically read the necessary
    bytes for header and data if a stream is provided on initialization.
    """

    def __init__(self, stream=None, **kwargs):
        """ Inititialize CCSDSPacket

        :param stream: A file object from which to read data (default: None)
        :type stream: file object

        :Keyword Arguments:
            hdr_data (bytes-like object): A bytes-like object containing 6-bytes
                of data that should be processed as a CCSDS Packet header.
            data (bytes-like object): The packet data field for the CCSDS
                packet. For consistency, this should be the length specified in
                the hdr_data per the CCSDS Packet format. However, this isn't
                enforced if these kwargs are used.

        """
        if stream:
            self.read(stream)
        else:
            d = bytearray(b'\x00\x00\x00\x00\x00\x00')
            self.hdr_data = kwargs.get('hdr_data', d)
            self._parse_header(self.hdr_data)
            self.data = kwargs.get('data', bytearray())

    @property
    def sclk(self):
        """ Extract sclk value from EurC CCSDS secondary header

        The first 10 bytes of a EurC CCSDS science data packet
        are the packet secondary header containing sclk course
        and fine time (first 48 bits) and the aid (remaining 4 bytes)
        """
        val = -1
        if self.sec_hdr_flag == 1:
            val = int.from_bytes(self.data[:6], byteorder='big')

        return val

    def _parse_header(self, hdr):
        """"""
        self.pkt_ver_num = (hdr[0] & 0xD0) >> 5
        self.pkt_type = (hdr[0] & 0x10) >> 4
        self.sec_hdr_flag = (hdr[0] & 0x08) >> 3
        self.apid = (int.from_bytes(hdr[0:2], 'big') & 0x07FF)
        self.seq_flags = (hdr[2] & 0xC0) >> 6
        self.pkt_seq_cnt = (int.from_bytes(hdr[2:4], 'big') & 0x3FFF)
        self.pkt_data_len = int.from_bytes(hdr[4:6], 'big')

    def read(self, stream):
        """ Read packet data from a stream

        :param stream: A file object from which to read data
        :type stream: file object

        """
        self.hdr_data = stream.read(6)
        if len(self.hdr_data) != 6:
            raise EOFError('CCSDS Header Read failed due to EOF')

        self._parse_header(self.hdr_data)
        # Packet Data Length is expressed as "number of packets in
        # packet data field minus 1"
        self.data = stream.read(self.pkt_data_len + 1)

    def to_bytes(self):
        return self.hdr_data + self.data

    def __repr__(self):
        return "<CCSDSPacket: apid={} pkt_seq_cnt={} pkt_data_len={}".format(
            self.apid, self.pkt_seq_cnt, self.pkt_data_len
        )


class ADPStream():
    """ ADP File Stream Object
    Provides an iterable native-like file interface for reading / writing
    ADP Files. ADPStream handles automatically reading / writing ADP file
    headers on initialization and facilitates easily reading
    :class:`CCSDSPacket`s from the file.
    Usage:
        A new stream is opened via the *open* class method. This returns an
        instance of the ADPStream with the requested file open. The ADPHeader
        will be automatically read or written depending on file mode.
    >>> adp = ADPStream.open('/path/to/example_adp.dat', 'rb')
    """

    def __init__(self, stream, mode='rb', header_metadata={}):
        """"""
        if mode.startswith('r'):
            self.header = ADPHeader(stream)
        elif mode.startswith('w') or (mode.startswith('a') and stream.tell() == 0):
            self.header = ADPHeader(**header_metadata)
            stream.write(self.header.pack())

        self._stream = stream

    @classmethod
    def open(cls, filename, mode='rb', **options):
        """Open an ADPStream to handle reading / writing ADP files
        Returns an instance of :class:`ADPStream` which wraps the builtin
        file interface. Binary mode is required so specifying in the mode flag
        is not required. The *options* kwargs are passed as ADP header
        metadata values so that default values can be provided when creating
        a new file.
        An :class:`ADPHeader` is automatically read or written from the stream
        when the :class:`ADPStream` instance is instantiated. Iterating over
        an :class:`ADPStream` will return each :class:`CCSDSPacket` in order.
        :param str filename: The file path/name this ADPStream references
        :param str mode: The file mode (default: 'rb')
        :param dict options): Kwargs values that will be treated as
            :class:`ADPHeader` metadata values so defaults can be provided
        :returns: Opened instance of ADPStream
        """
        mode = mode.replace('b', '') + 'b'
        return cls(builtins.open(filename, mode), mode, header_metadata=options)

    def __enter__(self):
        """"""
        return self

    def __exit__(self, type, value, traceback):
        """"""
        self.close()

    def __next__(self):
        """"""
        return self.next()

    def __iter__(self):
        """"""
        return self

    def next(self):
        """ Read the next CCSDS Packet during iteration """
        packet = self.read()

        if packet is None:
            raise StopIteration

        return packet

    def read(self):
        """ Read a CCSDS packet from the stream
        :returns: a :class:`CCSDSPacket` object containing the read data
            or None if EOF is reached.
        """
        try:
            pkt = CCSDSPacket(self._stream)
        except EOFError:
            pkt = None

        return pkt

    def write(self, payload):
        """ Write a binary payload to the file
        :param bytes payload: The bytes-like object to write to the stream
        """
        if isinstance(payload, CCSDSPacket):
            payload = payload.to_bytes()

        self._stream.write(payload)
        self._stream.flush()

    def close(self):
        """ Close the underlying file handler """
        self._stream.close()
