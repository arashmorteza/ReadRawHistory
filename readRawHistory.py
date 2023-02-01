__author__ = "Arash MORTEZA"
__version__ = "1.0.0.0"


import time
import logging
from random import gauss
from threading import Thread
from opcua import ua, uamethod, Server
try:
    from IPython import embed
except ImportError:
    import code

    def embed():
        _vars_ = globals()
        _vars_.update(locals())
        shell = code.InteractiveConsole(_vars_)
        shell.interact()


class dynamicObject(Thread):
    def __init__(self, nodes, samplingInterval):
        Thread.__init__(self)
        self._halt = False
        self.nodes = nodes
        self.samplingInterval = samplingInterval

    def stop(self):
        self._halt = True

    def run(self):
        while not self._halt:
            [node.set_value(gauss(0, 1) * 3 ** 2, ua.VariantType.Float) for node in self.nodes]
            time.sleep(self.samplingInterval)


class configurations:
    url = "opc.tcp://localhost:4840/open62541"
    uri = "http://programs.opcua.com/server/labs/rawReadHistory"
    name = "historicalAccess"
    allowRemoteAdmin = True
    authorizedUsers = {
        'Admin': 'pass',
    }
    securityPolicies = [
        ua.SecurityPolicyType.NoSecurity,
        ua.SecurityPolicyType.Basic256Sha256_SignAndEncrypt,
        ua.SecurityPolicyType.Basic256Sha256_Sign
    ]
    loggingLevel = logging.ERROR
    outStream = r'C:\Arash\outStream'


class open62541(configurations):
    def __init__(self, dictionary, **kwargs):

        self.history: list = []
        self.dynamicObjects: list = []

        self.historicalDataSamplingInterval = .001
        logging.basicConfig(level=configurations.loggingLevel)
        self.server = Server()
        self.server.set_endpoint(configurations.url)
        self.server.set_server_name(configurations.name)
        self.server.set_security_policy(configurations.securityPolicies)
        self.server.allow_remote_admin(configurations.allowRemoteAdmin)
        self.index = self.server.register_namespace(configurations.uri)
        self.object = self.server.nodes.objects.add_object(self.index, "Open62541")
        self.dynamicObjects.append(self.object.add_variable(self.index, "dynamicValue", "", ua.VariantType.Float))
        self.dynamicObjectThread = dynamicObject(self.dynamicObjects, self.historicalDataSamplingInterval)

        nodeId = ua.Argument()
        nodeId.Name = "IN 0:"
        nodeId.DataType = ua.NodeId(ua.ObjectIds.String)
        nodeId.ValueRank = -1
        nodeId.ArrayDimensions = []
        nodeId.Description = ua.LocalizedText("Node id")

        startTimestamp = ua.Argument()
        startTimestamp.Name = "IN 1:"
        startTimestamp.DataType = ua.NodeId(ua.ObjectIds.DateTime)
        startTimestamp.ValueRank = -1
        startTimestamp.ArrayDimensions = []
        startTimestamp.Description = ua.LocalizedText("Start timestamp")

        endTimestamp = ua.Argument()
        endTimestamp.Name = "IN 2:"
        endTimestamp.DataType = ua.NodeId(ua.ObjectIds.DateTime)
        endTimestamp.ValueRank = -1
        endTimestamp.ArrayDimensions = []
        endTimestamp.Description = ua.LocalizedText("End timestamp")

        self.object.add_method(self.index, "readRawHistory", self.readRawHistory, [nodeId, startTimestamp, endTimestamp], None)

        try:
            self.dynamicObjectThread.start()
            self.run()
            [self.server.historize_node_data_change(node) for node in self.dynamicObjects]
            print(f'[i]      Server is now running @ {configurations.url}')
            embed()
        finally:
            self.dynamicObjectThread.stop()
            self.stop()

    @uamethod
    def readRawHistory(self, parent, nodeId, startTimeStamp, endTimeStamp):
        self.history = self.server.get_node(ua.NodeId.from_string(nodeId)).read_raw_history(startTimeStamp, endTimeStamp)
        with open(fr'{configurations.outStream}\dataHistoryOutStream.csv', 'w') as outStreamFile:
            [outStreamFile.writelines(f'{data.SourceTimestamp}\t{data.Value.Value}\n') for data in self.history]

    def run(self):
        return self.server.start()

    def stop(self):
        return self.server.stop()


if __name__ == "__main__":
    mainConfigurations = configurations()
    _open62541 = open62541(mainConfigurations)
