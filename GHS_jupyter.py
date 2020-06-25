#!/usr/bin/env python
# coding: utf-8

# In[1]:


#get_ipython().run_line_magic('load_ext', 'autoreload')
#get_ipython().run_line_magic('autoreload', '2')
from multiprocessing import Process, Queue,Pool
from Graph import Graph
import threading
import queue as Q
import time
import sys


# In[2]:


class Edge():
    def __init__(self,src,dst,weight):
        self.src = src
        self.dst = dst
        self.weight = weight
        self.state = "Basic"   ### Other can be Branch and Rejected
        
    def __str__(self):
        return str(self.src) +"->" + str(self.dst) + ":" + str(self.weight) 
    def __gt__(self, other): 
        if(self.weight>other.weight): 
            return True
        else: 
            return False
    def __lt__(self,other):
        if(self.weight <=other.weight): 
            return True
        else: 
            return False
class Message():
    def __init__(self,type,src,dst, L=0,F=0,S=0,W=0):  ## S = state of node, F = fragment name L = Level Name
        self.type = type  ## type - Wakeup,Connect,Initiate,Test,Accept,Reject,Report,Change_root
        self.src = src 
        self.dst = dst
        self.L =L
        self.F = F
        self.S = S
        self.W = W
        
    def __str__(self):
        return self.type + ":" + str(self.src) + "->" + str(self.dst) + " L: " + str(self.L) +  " F: "+ str(self.F) +  " S: " + str(self.S) + " W: "+ str(self.W)



class Node():
    def __init__(self,id,V,adj_nodes,debug=False):
        self.id = id
        self.V= V
        self.state = "Sleeping"     ## Sleeping, Find,Found
        self.FN = -1
        self.LN = None
        self.debug = debug
        self.best_edge = None
        self.best_wt = sys.maxsize
        self.test_edge = None
        self.parent = None
        self.find_count = 0
        self.nbrs = {nbr[0]:Edge(self.id,nbr[0],nbr[1]) for nbr in adj_nodes}
        self.edges = list(self.nbrs.values())
        self.edges.sort()
        self.msg_count = {}
    def __str__(self):
        if self.LN is None:
            LN = "None"
        else:
            LN = str(self.LN)
        if self.best_edge is None:
            best_edge = "None"
        else:
            best_edge = str(self.best_edge)
        if self.parent is None:
            parent = "None"
        else:
            parent = str(self.parent)
        if self.test_edge is None:
            test_edge = "None"
        else:
            test_edge = str(self.test_edge)
        a =  "id:"+str(self.id)+" state,FN,LN: "+self.state +" "+ str(self.FN) +" " +LN + " best edge and weight:" + best_edge+ " " + str(self.best_wt)
        b = " parent:" + parent + " find count : " + str(self.find_count) + " test edge :" + test_edge
        return a +"\n" +b +"\n"
    def findEdgeIndexUsingNodeId(self,nodeid,type = "src"):
        for i in range(0, len(self.edges)):
            if type == "src":
                if self.edges[i].src == nodeid:
                    return i
            if type == "dst":
                if self.edges[i].dst == nodeid:
                    return i
    def main(self,msg_qs,outputq):
        done = False
        if self.debug:
            print("waking up, ", self.id)
        self.wakeup(msg_qs)
        while not done:
            try:

                msg = msg_qs[self.id].get(block=True,timeout=5)
                if self.debug:
                    print("Received message  ," +str(msg)+ " on "+ str(self.id)+ "\n"+ str(self))
                else:
                    time.sleep(0.0001)
                if msg.type in self.msg_count:
                    self.msg_count[msg.type] += 1
                else:
                    self.msg_count[msg.type] = 1

                incoming_edge_index = self.findEdgeIndexUsingNodeId(msg.src,"dst") ### src will have the node id which sent the message
                incoming_node = self.edges[incoming_edge_index].dst
                
                if msg.type == "Connect":
                    if msg.L < self.LN:
                        self.edges[incoming_edge_index].state = "Branch"
                        initiate_msg = Message("Initiate",self.id,incoming_node,L=self.LN,F=self.FN,S=self.state)
                        msg_qs[incoming_node].put(initiate_msg)
                        if self.state == "Find":
                            self.find_count += 1
                    elif self.edges[incoming_edge_index].state == "Basic":
                        msg_qs[self.id].put(msg)
                    else:
                        initiate_msg = Message("Initiate",self.id,incoming_node,L=self.LN+1,F=self.edges[incoming_edge_index].weight,S="Find")
                        msg_qs[incoming_node].put(initiate_msg)
                        
                elif msg.type == "Initiate":
                    self.LN = msg.L
                    self.FN = msg.F
                    self.state = msg.S
                    self.parent = incoming_node
                    self.best_edge = None
                    self.best_wt = sys.maxsize
                    for edge in self.edges:
                        if edge.dst != incoming_node and edge.state == 'Branch':
                            initiate_msg = Message("Initiate",self.id,edge.dst,L=self.LN,F=self.FN,S=self.state)
                            msg_qs[edge.dst].put(initiate_msg)
                            
                            if self.state == "Find":
                                self.find_count += 1
                    if self.state == "Find":
                        self.test(msg_qs)
                        
                elif msg.type == "Test":
                    if msg.L > self.LN: ###  put to wait
                        msg_qs[self.id].put(msg)
                        time.sleep(0.001)
                        #time.sleep(2)
                    elif msg.F != self.FN:  ## different fragment so accept
                        accept_msg = Message("Accept",self.id,incoming_node)
                        msg_qs[incoming_node].put(accept_msg)
                    else:  ### in same fragment 
                        if self.edges[incoming_edge_index].state == "Basic":
                            self.edges[incoming_edge_index].state = "Rejected"
                        if incoming_node != self.test_edge:  ### can be removed anyway just a optimization
                            reject_msg = Message("Reject",self.id,incoming_node)
                            msg_qs[incoming_node].put(reject_msg)
                        else:   #### received reject from edge where test was sent so now do  test on another edge
                            self.test(msg_qs)
                elif msg.type == "Accept":
                    self.test_edge= None
                    if self.edges[incoming_edge_index].weight < self.best_wt:
                        self.best_edge = incoming_node
                        self.best_wt = self.edges[incoming_edge_index].weight
                    self.report(msg_qs)
                elif msg.type == "Reject":
                    if self.edges[incoming_edge_index].state == "Basic":
                        self.edges[incoming_edge_index].state = "Rejected"
                    self.test(msg_qs)
                elif msg.type == "Report":
                    if incoming_node != self.parent:
                        self.find_count  = self.find_count -1
                        if msg.W < self.best_wt:
                            self.best_wt = msg.W
                            self.best_edge = incoming_node
                        self.report(msg_qs)
                    else:
                        if self.state =="Find":
                            msg_qs[self.id].put(msg)
                            time.sleep(0.0001)
                        elif msg.W > self.best_wt:   ### core edges and need to switch path
                            self.change_root(msg_qs)
                        elif msg.W == sys.maxsize and self.best_wt == sys.maxsize:
                            done = True
                            outputq.put([self.id,self.parent,self.msg_count,self.LN])
                            if self.debug:
                                print("Nothing to do for , ", self.id , " ",self.parent, " best edge", self.best_edge)
                elif msg.type == "Change_root":
                    self.change_root(msg_qs)

            
            except Q.Empty:
                #time.sleep(2)
                #continue
                outputq.put([self.id,self.parent,self.msg_count,self.LN])
                if self.debug:
                    print("No new message, my parent is, ", self.id ," ", self.parent," best edge "," ", self.best_edge)
                return
            
            
    def wakeup(self,msg_qs):
        self.edges[0].state = "Branch"
        self.find_count = 0
        self.LN = 0
        self.state = "Found"
        connect_msg = Message("Connect",self.id,self.edges[0].dst,L=0)
        msg_qs[self.edges[0].dst].put(connect_msg)
        if self.debug:
            print("wakeup : sending message from "+ str(self.id) + "to, "+ str(self.edges[0].dst))
            
    def test(self,msg_qs):
        flag_found= False
        for edge in self.edges:
            if edge.state == "Basic" and not flag_found:
                flag_found = True
                self.test_edge = edge.dst
                test_msg = Message("Test",self.id,edge.dst,L=self.LN,F=self.FN)
                msg_qs[edge.dst].put(test_msg)
                
        if not flag_found:  ### No test send, so simply report to parent.
            self.test_edge = None
            self.report(msg_qs)
    
    def report(self,msg_qs):
        if self.find_count ==0 and self.test_edge is None:
            self.state = 'Found'
            report_msg = Message("Report",self.id,self.parent,S=0,F=0,L=0,W=self.best_wt)
            msg_qs[self.parent].put(report_msg)
    
    def change_root(self,msg_qs):
        best_edge_index = self.findEdgeIndexUsingNodeId(self.best_edge,"dst")
        if self.edges[best_edge_index].state == "Branch":
            change_root_msg = Message("Change_root",self.id,self.best_edge)
            msg_qs[self.best_edge].put(change_root_msg)
        else:
            self.edges[best_edge_index].state = 'Branch'
            connect_msg= Message("Connect",self.id,self.best_edge,L=self.LN)
            msg_qs[self.best_edge].put(connect_msg)
            
        
        


# In[5]:

if __name__== "__main__":
    debug = False
    debug_msg = True
    graph = Graph(sys.argv[1])
    graph.transform_graph()
    qs = []
    if debug:
        print("Opening queues, " , graph.V)
    for i in range(0, graph.V):
        qs.append(Queue())
    if debug:
        print("Opened queues")
    outputq = Queue()
    nodes= [Node(id,graph.V,graph.tgraph[id],debug) for id in range(0,graph.V)]
    processes = [Process(target=nodes[id].main, args=(qs,outputq,)) for id in range(graph.V)]
    # Run processes
    for p in processes:
        p.start()
    if debug:
        print("started all process")
    # Exit the completed processes
    for p in processes:
        p.join()
    if debug:
        print("Joined all process")
    for p in processes:
        p.close()
    output_edges = [outputq.get() for p in processes]
    messages_node_wise = [item[2] for item in output_edges]
    output_edges = [ (graph.id_to_node[edge[0]],graph.id_to_node[edge[1]]) for edge in output_edges]
    # if debug:
    #     print(output_edges)
    output_edges = [ graph.graph_dict[edge[0]][edge[1]] for edge in output_edges]
    output_edge_dup = []
    wt_dict  = {}
    for edge in output_edges:
        if edge[2] not in wt_dict:
            wt_dict[edge[2]] = 1
            output_edge_dup.append(edge)
            
    output_edges = output_edge_dup
    output_edges.sort(key = lambda val: val[2])
    f = open("output","w")
    for edge in output_edges:
        f.write("("+ str(edge[0])+", "+str(edge[1])+", "+ str(edge[2])+")")
        print("("+ str(edge[0])+", "+str(edge[1])+", "+ str(edge[2])+")")
        f.write("\n")
    f.close()
    
    
    if debug_msg:
        print("Messages complexity")
        ct= 0 
        msg_dict = {}
        for msg_node in messages_node_wise:
            
            for key,value in msg_node.items():
                if key in msg_dict:
                    msg_dict[key] += value
                else:
                    msg_dict[key] = value
    
        for key,value in msg_dict.items():
            print(key, "->" , value)
            ct += value
            
        print("total message passed, ", ct)
    
