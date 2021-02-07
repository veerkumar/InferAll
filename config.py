
'''
	This file contains initial configuration needed for the simulator
'''

class Configuration_cls(object):
	"""docstring for Configuration_cls"""
	def __init__(self, arg):
		super(Configuration_cls, self).__init__()
		self.arg = arg
		self.MAX = 1000000
		self.MILL_TO_HR = 1/3600000
		self.batch_sz = [1,16,32,64,128,256,512,1024,2048,4096,8192,16384]
		# Accuracy picked from https://github.com/dmlc/mxnet-model-gallery and 
		# https://github.com/knjcode/mxnet-finetuner/blob/master/docs/pretrained_models.md
		self.top1_accuracy = [54.5, 55.4, 71.0, 72.5, 77.9]
		self.top5_accuracy = [78.3,78.8,89.8, 90.8,93.8]
		self.models_accuracy = [60,70,80,90,95]
		self.lambda_models = ["caffenet","squeeznet", "vggnet16","inception", "resnet200",]
		#self.lambda_models = ["caffenet", "inception", "resnet200", "squeeznet","vggnet16"]
		self.vm_models = ["caffenet","vggnet", "resnet200",]
		#vm_available_memory = [3696,7610,15438]
		self.vm_available_memory = [7610,15438]
		self.vm_cost = [0.085, 0.17, 0.34] # $/hour
		self.lambda_available_memory = [256,512,1024,2048,3008]
		# Model x Memory size x BATCH
		self.lambda_latency = [[[MAX,MAX,MAX,MAX,MAX,MAX,MAX,MAX,MAX,MAX,MAX,MAX],
							[MAX,MAX,MAX,MAX,MAX,MAX,MAX,MAX,MAX,MAX,MAX,MAX],
							[MAX,MAX,MAX,MAX,MAX,MAX,MAX,MAX,MAX,MAX,MAX,MAX],
							[1153,1153,1153,1153,1153,MAX,MAX,MAX,MAX,MAX,MAX,MAX],
							[MAX,MAX,MAX,MAX,MAX,MAX,MAX,MAX,MAX,MAX,MAX,MAX]
						   ],[[MAX,MAX,MAX,MAX,MAX,MAX,MAX,MAX,MAX,MAX,MAX,MAX],
						   	  [1118.086,1118.086,1118.086,1118.086,1118.086,MAX,MAX,MAX,MAX,MAX,MAX,MAX,MAX],
						      [MAX,MAX,MAX,MAX,MAX,MAX,MAX,MAX,MAX,MAX,MAX,MAX],
						      [900,900,900,900,900,MAX,MAX,MAX,MAX,MAX,MAX,MAX],
						      [MAX,MAX,MAX,MAX,MAX,MAX,MAX,MAX,MAX,MAX,MAX,MAX],
						   ],[
						   ],[
						   ],[
						   ]]
		
		# Convert to Model X memory x batch
		self.vm_latency = [[[19.8,33.305,41.54,62.551,122.298,205.596,427.821,764.486,1627.225,2800.737,5573.297,MAX],
							 [12.6,24.098,36.61,56.395,94.496,199.249,365.824,721.434,1536.959,2472.725,5016.331,10164.666]
							],[[131.6,143.304,151.566,172.992,235.326,339.47,546.428,937.599,1826.644,3060.124,5544.018,MAX],
							   [64.195,75.282,85.781,128.78,155.629,257.018,451.782,838.022,1635.931,3099.905,6139.913,12353.488]
							],[[110.938,139.928,150.047,204.313,240.959,330.585,536.728,876.647,1664.337,3255.478,6370.758,MAX],
							   [65.5,77.61,84.868,105.741,164.769,227.66,421.594,808.011,1628.959,3184.752,5122.492,10173.345]
							]]
		# # Dimenstion: type_VM(large, xlarge,xxlarge) x Model(each with different accuracy level)  x Batch_Size
		# self.vm_latencey = 	[[[19.8,33.305,41.54,62.551,122.298,205.596,427.821,764.486,1627.225,2800.737,5573.297,MAX],
		# 				  [131.6,143.304,151.566,172.992,235.326,339.47,546.428,937.599,1826.644,3060.124,5544.018,MAX],
		# 				  [748.938,139.928,150.047,204.313,240.959,330.585,536.728,876.647,1664.337,3255.478,6370.758,MAX],
		# 				 ],[[12.6,24.098,36.61,56.395,94.496,199.249,365.824,721.434,1536.959,2472.725,5016.331,10164.666],
		# 				  [64.195,75.282,85.781,128.78,155.629,257.018,451.782,838.022,1635.931,3099.905,6139.913,12353.488],
		# 				  [65.5,77.61,84.868,105.741,164.769,227.66,421.594,808.011,1628.959,3184.752,5122.492,10173.345],
		# 				]]
		#RAM consumption is identical accross different VM size
		# self.vm_memory = [[1237.910156, 1247.515625, 1257.191406, 1276.148438, 1313.308594,1387.199219,1534.308594,1828.882813,2419.261719,3599.527344,5963.554688,10685.46094],
		# 			 [1401.898438,1402.128906,1407.542969,1408.980469,1463.40625,1536.988281,1684.644531,1979.703125,2568.851563,3747.894531,6106.210938,10822.10547],
		# 			 [2476.695313,2486.550781,2495.710938,2514.160156,2550.828125,2624.867188,2772.277344,3068.171875,3657.363281,4837.628906, 7199.375, 11922.63672],
		# 			]
		self.vm_memory = [[1401.898438,1402.128906,1407.542969,1408.980469,1463.40625,1536.988281,1684.644531,1979.703125,2568.851563,3747.894531,6106.210938,10822.10547],
					 [2476.695313,2486.550781,2495.710938,2514.160156,2550.828125,2624.867188,2772.277344,3068.171875,3657.363281,4837.628906, 7199.375, 11922.63672],
					]
		self.trace_dir = "traces/"

		self.INITIAL_WORKERS = 10
		self.VM_PREDICTION = 0
		self.load_tracking = 0
		self.MONITOR_INTERVAL = int(100000)
		self.start_up_delay = int(100000)
		self.last_task = 0
		self.SLO =  300  # milisecond

		os.system("rm -rf outputs")
		os.mkdir('outputs')
		self.finished_file_path = os.path.join('outputs', 'finished_file.csv')
		self.all_tasks_path = os.path.join('outputs', 'all_tasks.csv')
		self.VM_stats_path = os.path.join('outputs', 'VM_stats.csv')
		self.load_file_path =  os.path.join('outputs', 'load')
		self.cost_path = os.path.join('outputs', 'cost')
		self.optimiztion_type = 0
		self.scheduling_type = 0
		self.rescheduling_limit = 4
		self.scheduler_wakeup_timer = 100 # in millisecond



print ("Lambda latency with batch size [1,16,32,64,128,256,512,1024,2048,4096,8192]")
for i, element in enumerate(vm_latencey):
	print(models[i] + ": ", end='')
	print (element)