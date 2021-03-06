import os,sys
import shutil

from argparse import ArgumentParser
from aoidb.database import AoiDB,AoiDB2
from json import dump

parser = ArgumentParser()
parser.add_argument(
  'command',
  help='new: create new project',
  type=str, default=''
)
parser.add_argument(
  'project_name',
  help='The name of your project',
  type=str, default='Unnamed'
)
parser.add_argument(
  '-N','--name',
  help='The name of your database(Not project name)',
  type=str, default=''
)
parser.add_argument(
  '-V','--version',
  help='The version of your database(1 or 2)',
  type=int, default=2
)
parser.add_argument(
  '-I','--interface',
  help='create a interface server(with \'new\' command).',
  action='store_true', default=False
)

def new(args):
  p_name = args.project_name
  if p_name=='Unnamed':
    d_name = 'Unnamed'
  elif args.name=='':
    d_name = args.project_name
  else:
    d_name = args.name
  
  run_path = os.getcwd()+f'/{p_name}'
  
  check = 'Y'
  exsited = False
  if os.path.isdir(run_path):
    exsited = True
    check = input('The folder has been existed, do you want to overwrit it?(Y/n)')
  
  if check.upper()=='Y':
    if exsited:
      shutil.rmtree(run_path)

    if args.interface:
      file_path = os.path.split(os.path.abspath(__file__))[0]+'/template/interface'
      shutil.copytree(file_path, run_path)
      config = {
        "name": p_name,
        "ip": "127.0.0.1",
        "port": 12345,
        "path": run_path,
        "configs": {}
      }
      with open(run_path+"/config.json",'w',encoding='utf-8') as f:
        dump(config, f, indent=2)
    else:
      file_path = os.path.split(os.path.abspath(__file__))[0]+'/template/project'
      shutil.copytree(file_path,run_path)
      os.remove(run_path+'/database/empty.txt')
    
      if args.version==1:
        new_DB = AoiDB(d_name)
        new_DB.save(run_path+f'/database/{p_name}')
      elif args.version==2:
        new_DB = AoiDB2(d_name)
        new_DB.save(run_path+f'/database/{p_name}')
      else:
        print('Version should be 1 or 2!')
        return 
      config = [{}]
      config[0]["database_option"]={
        "name": d_name,
        "path": run_path+f'/database/{p_name}.aoi{"" if args.version==1 else 2}'
      }
      config[0]["IP"] = "127.0.0.1"
      config[0]["Port"] = 38738
      config[0]['version'] = args.version

      with open(run_path+"/config.json",'w',encoding='utf-8') as f:
        dump(config,f,indent=2)
  else:
    return
  print('Done!')

def main():
  args = parser.parse_args()
  if args.command!='':
    globals()[args.command](args)
  

if __name__=='__main__':
  main()