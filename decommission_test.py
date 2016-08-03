import time

from ccmlib.node import NodetoolError
from dtest import Tester, debug
from threading import Thread

class TestDecommission(Tester):

    def __init__(self, *args, **kwargs):
        kwargs['cluster_options'] = {'start_rpc': 'true'}
        # Ignore these log patterns:
        self.ignore_log_patterns = [
        	r'Streaming error occurred',
        	r'Error while decommissioning node'
        ]
        Tester.__init__(self, *args, **kwargs)

    def simple_decommission_test(self):
    	"""
    	Test basic decommission operation and concurrent decommission is not allowed
    	"""
    	# Create 2 nodes for decommission operation
    	cluster = self.cluster
    	cluster.populate(2).start(wait_other_notice=True)
    	
    	node2 = cluster.nodes['node2']
    	node2.stress(['write', 'n=10K', 'no-warmup', 'cl=ONE', '-schema', 'replication(factor=1)', '-rate', 'threads=50'])
        cluster.flush()
        
        mark = node2.mark_log()
        
        def decommission():
            node2.nodetool('decommission')
            
        # Launch first decommission in a external thread
        t = Thread(target=decommission)
        t.start()
        
        # Make sure first decommission is initialized before second decommission
        time.sleep(1)
        
        # Launch a second decommission, should fail
        with self.assertRaises(NodetoolError):
            node2.nodetool('decommission')
            
        # Check data is correctly fowarded to node1 after node2 is decommissioned
        t.join()
        node2.watch_log_for('DECOMMISSIONED', from_mark=mark)
        node1 = cluster.nodes['node1']
        stdout, stderr = node1.stress(['read', 'n=10k', 'cl=ONE', 'no-warmup', '-schema',
                                       'replication(factor=2)', '-rate', 'threads=8'],
                                       capture_output=True)
        if stdout is not None:
            self.assertNotIn("FAILURE", stdout)

    def resumable_decommission_test(self):
    	"""
    	@jira-ticket CASSANDRA-12008

    	Test decommission operation is resumable
    	"""
    	# Set-up cluster with 3 nodes
    	cluster = self.cluster
    	cluster.set_configuration_options(values={'stream_throughput_outbound_megabits_per_sec': 1})
    	cluster.populate(3).start(wait_other_notice=True)

    	node1 = cluster.nodes['node1']
    	node2 = cluster.nodes['node2']

        node2.stress(['write', 'n=10K', 'no-warmup', 'cl=TWO', '-schema', 'replication(factor=2)', '-rate', 'threads=50'])
        cluster.flush()

        # Kill node1 while decommissioning to make fist decommission fail
        def InterruptDecommission():
            node2.watch_log_for('DECOMMISSIONING', filename='debug.log')
            node1.stop(gently=False)

        t = Thread(target=InterruptDecommission)
        t.start()

        # Execute first rebuild, should fail
        with self.assertRaises(NodetoolError):
            node2.nodetool('decommission')

        t.join()

        # Relaunch node1 and decommission again
        node1.start()
        mark = node2.mark_log()
        node2.nodetool('decommission')

        # Check decommision is done and we are skipping transfereed ranges
        node2.watch_log_for('DECOMMISSIONED', from_mark=mark)
        node2.watch_log_for('Skipping transferred range', from_mark=mark, filename='debug.log')

        # Check data is correctly forwarded to node1
        stdout, stderr =  node1.stress(['read', 'n=10k', 'cl=ONE', 'no-warmup', '-schema',
                                        'replication(factor=2)', '-rate', 'threads=8'],
                                        capture_output=True)

        if stdout is not None:
            self.assertNotIn("FAILURE", stdout)
