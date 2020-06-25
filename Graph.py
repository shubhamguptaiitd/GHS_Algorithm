class Graph():
    def __init__(self,filename=None,V=None):
        self.graph = {}
        if V is not None:
            self.V = V
        else:
            data = open(filename).read().splitlines()
            self.V = int(data[0])
            self.graph_dict = {}
            if data[len(data)-1] == '':
                data = data[:len(data)-1]
            for i in range(1, len(data)):
                row = data[i]
                row = row.replace(")","").replace("(","")
                row = [int(item) for item in row.split(",")]
                start = int(row[0])
                end = int(row[1])
                weight = int(row[2])
                self.add_edge(start,end,weight)
                if start  not in self.graph_dict:
                    self.graph_dict[start]= {end:(start,end,weight)}
                else:
                    self.graph_dict[start][end] = (start,end,weight)
                if end  not in self.graph_dict:
                    self.graph_dict[end]= {start:(start,end,weight)}
                else:
                    self.graph_dict[end][start] = (start,end,weight)

    def transform_graph(self):
        self.id_to_node = {}
        self.node_to_id = {}
        nodes = list(self.graph.keys())
        nodes.sort()
        index = range(0,self.V)
        self.node_to_id = {a:b for a,b in zip(nodes,index)}
        self.id_to_node = {b:a for a,b in zip(nodes,index)}
        self.tgraph = {}
        for key,value in self.graph.items():
            key = self.node_to_id[key]
            self.tgraph[key] = [[self.node_to_id[item[0]],item[1]]  for item in value]
            
            
            
    def find_weight(self,start,end):
        for i in self.graph[start]:
            if i[0] == end:
                return i[1]          
    def add_edge(self,start,end,weight):
        if start in self.graph:
            self.graph[start].append([end,weight])
        else:
            self.graph[start]  = [[end,weight]]
        if end in self.graph:
                self.graph[end].append([start,weight])
        else:
            self.graph[end]  = [[start,weight]] 