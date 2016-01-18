class realtimeProc:
    def __init__(self, manager):
        self._manager = manager

    def proc(self, bds):
        print(len(bds))