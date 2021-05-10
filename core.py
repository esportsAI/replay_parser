import mpyq
import heroprotocol.versions as protocol_versions


class Replay(object):
    def __init__(self, replay_path):
        self.archive = mpyq.MPQArchive(replay_path)

        # Read Header
        contents = self.archive.header['user_data_header']['content']
        self.header = protocol_versions.latest().decode_replay_header(contents)

        # Determine protocol version
        base_build = self.header['m_version']['m_baseBuild']
        self.protocol = protocol_versions.build(base_build)

    def read_details(self):
        contents = self.archive.read_file('replay.details')

        return self.protocol.decode_replay_details(contents)

    def read_init_data(self):
        contents = self.archive.read_file('replay.initData')

        return self.protocol.decode_replay_initdata(contents)

    def read_events(self,
                    event_types=['game', 'message', 'tracker', 'attributes']):
        events = {}

        for event_type in event_types:
            event_attr = f'decode_replay_{event_type}_events'
            if not hasattr(self.protocol, event_attr):
                continue

            events[event_type] = []

            contents = self.archive.read_file(f'replay.{event_type}.events')
            result = getattr(self.protocol, event_attr)

            for event in result(contents):
                events[event_type].append(event)

        return events
