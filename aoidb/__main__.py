import os,sys
import shutil

from argparse import ArgumentParser
from aoidb.database import AoiDB
from json import dump

parser = ArgumentParser()
parser.add_argument('command',
										help='Run Your Server(Need config.json existed)',
										type=str,default='')
parser.add_argument('project_name',
										help='The name of your project',
										type=str,default='Unnamed')
parser.add_argument('-N','--name',
										help='The name of your database(Not project name)',
										type=str,default='')

def new(args):
	p_name = args.project_name
	if p_name=='Unnamed':
		d_name = 'Unnamed'
	elif args.name=='':
		d_name = args.project_name
	else:
		d_name = args.name
	
	file_path = os.path.split(os.path.abspath(__file__))[0]+'/template/project'
	run_path = os.getcwd()+f'/{p_name}'
	
	check = 'Y'
	exsited = False
	if os.path.isdir(run_path):
		exsited = True
		check = input('The folder has been existed, do you want to overwrit it?(Y/n)')
	
	if check.upper()=='Y':
		if exsited:
			shutil.rmtree(run_path)
		shutil.copytree(file_path,run_path)
		
		new_DB = AoiDB(d_name)
		new_DB.save(run_path+f'/database/{p_name}')

		config = [{}]
		config[0]["database_option"]={"name": d_name,
																	"path": run_path+f'/database/{p_name}.aoi'}
		config[0]["IP"] = "127.0.0.1"
		config[0]["Port"] = 38738

		with open(run_path+"/config.json",'w',encoding='utf-8') as f:
			dump(config,f)
	else:
		return
	print('Done!')

def main():
	args = parser.parse_args()
	if args.command!='':
		globals()[args.command](args)
	

if __name__=='__main__':
	main()