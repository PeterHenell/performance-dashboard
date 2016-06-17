class WMISource:

    def get_sources(self):
        return []
#
#
# '''
# Original Author/Source:
# http://monkut.webfactional.com/blog/archive/2009/1/21/windows-process-memory-logging-python
# http://stackoverflow.com/questions/276052/how-to-get-current-cpu-and-ram-usage-in-python
# Monitor window processes
# derived from:
# >for sys available mem
# http://msdn2.microsoft.com/en-us/library/aa455130.aspx
# > individual process information and python script examples
# http://www.microsoft.com/technet/scriptcenter/scripts/default.mspx?mfr=true
# NOTE: the WMI interface/process is also available for performing similar tasks
#         I'm not using it here because the current method covers my needs, but if someday it's needed
#         to extend or improve this module, then may want to investigate the WMI tools available.
#         WMI for python:
#         http://tgolden.sc.sabren.com/python/wmi.html
# '''
#
# __revision__ = 3
#
# import win32com.client
# from ctypes import *
# from ctypes.wintypes import *
# import pythoncom
# import pywintypes
# import datetime
#
#
# class MEMORYSTATUS(Structure):
#     _fields_ = [
#                 ('dwLength', DWORD),
#                 ('dwMemoryLoad', DWORD),
#                 ('dwTotalPhys', DWORD),
#                 ('dwAvailPhys', DWORD),
#                 ('dwTotalPageFile', DWORD),
#                 ('dwAvailPageFile', DWORD),
#                 ('dwTotalVirtual', DWORD),
#                 ('dwAvailVirtual', DWORD),
#                 ]
#
#
# def winmem():
#     x = MEMORYSTATUS() # create the structure
#     windll.kernel32.GlobalMemoryStatus(byref(x)) # from cytypes.wintypes
#     return x
#
#
# class process_stats:
#     '''process_stats is able to provide counters of (all?) the items available in perfmon.
#     Refer to the self.supported_types keys for the currently supported 'Performance Objects'
#     To add logging support for other data you can derive the necessary data from perfmon:
#     ---------
#     perfmon can be run from windows 'run' menu by entering 'perfmon' and enter.
#     Clicking on the '+' will open the 'add counters' menu,
#     From the 'Add Counters' dialog, the 'Performance object' is the self.support_types key.
#     --> Where spaces are removed and symbols are entered as text (Ex. # == Number, % == Percent)
#     For the items you wish to log add the proper attribute name in the list in the self.supported_types dictionary,
#     keyed by the 'Performance Object' name as mentioned above.
#     ---------
#     NOTE: The 'NETFramework_NETCLRMemory' key does not seem to log dotnet 2.0 properly.
#     Initially the python implementation was derived from:
#     http://www.microsoft.com/technet/scriptcenter/scripts/default.mspx?mfr=true
#     '''
#     def __init__(self,process_name_list=[],perf_object_list=[],filter_list=[]):
#         '''process_names_list == the list of all processes to log (if empty log all)
#         perf_object_list == list of process counters to log
#         filter_list == list of text to filter
#         print_results == boolean, output to stdout
#         '''
#         pythoncom.CoInitialize() # Needed when run by the same process in a thread
#
#         self.process_name_list = process_name_list
#         self.perf_object_list = perf_object_list
#         self.filter_list = filter_list
#
#         self.win32_perf_base = 'Win32_PerfFormattedData_'
#
#         # Define new datatypes here!
#         self.supported_types = {
#                                     'NETFramework_NETCLRMemory':    [
#                                                                         'Name',
#                                                                         'NumberTotalCommittedBytes',
#                                                                         'NumberTotalReservedBytes',
#                                                                         'NumberInducedGC',
#                                                                         'NumberGen0Collections',
#                                                                         'NumberGen1Collections',
#                                                                         'NumberGen2Collections',
#                                                                         'PromotedMemoryFromGen0',
#                                                                         'PromotedMemoryFromGen1',
#                                                                         'PercentTimeInGC',
#                                                                         'LargeObjectHeapSize'
#                                                                      ],
#
#                                     'PerfProc_Process':              [
#                                                                           'Name',
#                                                                           'PrivateBytes',
#                                                                           'ElapsedTime',
#                                                                           'IDProcess',# pid
#                                                                           'Caption',
#                                                                           'CreatingProcessID',
#                                                                           'Description',
#                                                                           'IODataBytesPersec',
#                                                                           'IODataOperationsPersec',
#                                                                           'IOOtherBytesPersec',
#                                                                           'IOOtherOperationsPersec',
#                                                                           'IOReadBytesPersec',
#                                                                           'IOReadOperationsPersec',
#                                                                           'IOWriteBytesPersec',
#                                                                           'IOWriteOperationsPersec'
#                                                                       ]
#                                 }
#
#     def get_pid_stats(self, pid):
#         this_proc_dict = {}
#
#         pythoncom.CoInitialize() # Needed when run by the same process in a thread
#         if not self.perf_object_list:
#             perf_object_list = self.supported_types.keys()
#
#         for counter_type in perf_object_list:
#             strComputer = "."
#             objWMIService = win32com.client.Dispatch("WbemScripting.SWbemLocator")
#             objSWbemServices = objWMIService.ConnectServer(strComputer,"root\cimv2")
#
#             query_str = '''Select * from %s%s''' % (self.win32_perf_base,counter_type)
#             colItems = objSWbemServices.ExecQuery(query_str) # "Select * from Win32_PerfFormattedData_PerfProc_Process")# changed from Win32_Thread
#
#             if len(colItems) > 0:
#                 for objItem in colItems:
#                     if hasattr(objItem, 'IDProcess') and pid == objItem.IDProcess:
#
#                             for attribute in self.supported_types[counter_type]:
#                                 eval_str = 'objItem.%s' % (attribute)
#                                 this_proc_dict[attribute] = eval(eval_str)
#
#                             this_proc_dict['TimeStamp'] = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S.') + str(datetime.datetime.now().microsecond)[:3]
#                             break
#
#         return this_proc_dict
#
#
#     def get_stats(self):
#         '''
#         Show process stats for all processes in given list, if none given return all processes
#         If filter list is defined return only the items that match or contained in the list
#         Returns a list of result dictionaries
#         '''
#         pythoncom.CoInitialize() # Needed when run by the same process in a thread
#         proc_results_list = []
#         if not self.perf_object_list:
#             perf_object_list = self.supported_types.keys()
#
#         for counter_type in perf_object_list:
#             strComputer = "."
#             objWMIService = win32com.client.Dispatch("WbemScripting.SWbemLocator")
#             objSWbemServices = objWMIService.ConnectServer(strComputer,"root\cimv2")
#
#             query_str = '''Select * from %s%s''' % (self.win32_perf_base,counter_type)
#             colItems = objSWbemServices.ExecQuery(query_str) # "Select * from Win32_PerfFormattedData_PerfProc_Process")# changed from Win32_Thread
#
#             try:
#                 if len(colItems) > 0:
#                     for objItem in colItems:
#                         found_flag = False
#                         this_proc_dict = {}
#
#                         if not self.process_name_list:
#                             found_flag = True
#                         else:
#                             # Check if process name is in the process name list, allow print if it is
#                             for proc_name in self.process_name_list:
#                                 obj_name = objItem.Name
#                                 if proc_name.lower() in obj_name.lower(): # will log if contains name
#                                     found_flag = True
#                                     break
#
#                         if found_flag:
#                             for attribute in self.supported_types[counter_type]:
#                                 eval_str = 'objItem.%s' % (attribute)
#                                 this_proc_dict[attribute] = eval(eval_str)
#
#                             this_proc_dict['TimeStamp'] = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S.') + str(datetime.datetime.now().microsecond)[:3]
#                             proc_results_list.append(this_proc_dict)
#
#             except pywintypes.com_error, err_msg:
#                 # Ignore and continue (proc_mem_logger calls this function once per second)
#                 continue
#         return proc_results_list
#
#
# def get_sys_stats():
#     ''' Returns a dictionary of the system stats'''
#     pythoncom.CoInitialize() # Needed when run by the same process in a thread
#     x = winmem()
#
#     sys_dict = {
#                     'dwAvailPhys': x.dwAvailPhys,
#                     'dwAvailVirtual':x.dwAvailVirtual
#                 }
#     return sys_dict
#
#
# if __name__ == '__main__':
#     # This area used for testing only
#     sys_dict = get_sys_stats()
#
#     stats_processor = process_stats(process_name_list=['process2watch'],perf_object_list=[],filter_list=[])
#     proc_results = stats_processor.get_stats()
#
#     for result_dict in proc_results:
#         print result_dict
#
#     import os
#     this_pid = os.getpid()
#     this_proc_results = stats_processor.get_pid_stats(this_pid)
#
#     print 'this proc results:'
#     print this_proc_results