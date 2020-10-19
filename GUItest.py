from tkintertable.Tables import *
from tkintertable.TableModels import TableModel
from tkinter.filedialog import askdirectory,askopenfilename
from tkinter import *

import os,sys
import AoiDB
import threading as th
from time import time,sleep
from copy import copy,deepcopy

class Table(TableCanvas):
	def adjustColumnWidths(self):
		try:
			fontsize = self.thefont[1]
		except:
			fontsize = self.fontsize
	   
		scale = 8 * float(fontsize)/12
	   
		for col in range(self.cols):
			colname = self.model.getColumnName(col)
	   
			if colname in self.model.columnwidths:
				w = self.model.columnwidths[colname]
			else:
				w = self.cellwidth
	   
			maxlen = self.model.getlongestEntry(col)
			size = maxlen * scale
	   
			if size < w:
				continue
			#if size >= self.maxcellwidth:
			#	size = self.maxcellwidth
			self.model.columnwidths[colname] = size*1.2 #I changed the calculation to a static float
	   
		return

class AoiViewer:
	def __init__(self,DB = AoiDB.AoiPy(),w=1400,h=900):
		self.DB = DB
		
		self.w, self.h = w,h
		self.tk = Tk()
		self.tk.geometry(f'{w}x{h}')
		self.tk.resizable(height = 0, width = 0)
		self.tk.title(DB.name)
		self.tk.configure(bg='white')
		
		self.path_label = Label(self.tk, text='資料庫路徑:', 
								bg='white', width=10, font=('Arial',18))
		self.path_label.place(x=0,y=0)
		self.path_text = StringVar()
		self.path = Entry(self.tk, textvariable=self.path_text, 
							width=79, font=('Arial', 18))
		self.path.place(x=133,y=0)
		
		self.column = Label(self.tk, text='選擇顯示欄位(以,隔開):', 
							bg='white', width=19, font=('Arial',18))
		self.column.place(y=40)
		self.column_text = StringVar()
		self.column_choose = Entry(self.tk, textvariable=self.column_text, 
									width=70, font=('Arial', 18))
		self.column_choose.place(y=40,x=250)
		
		self.change_path = Button(self.tk, text='選擇檔案', bg='white',
									command=self.change_path, font=('Arial', 18))
		self.change_path.place(y=80)
		self.load = Button(self.tk, text='載入資料庫', bg='white',
							command=self.load, font=('Arial', 18))
		self.load.place(y=80,x=140)
		
		
		self.frame = Frame(self.tk)
		self.frame.place(y=125)
		
		self.model = TableModel()
		self.model.importDict({' ':{' ': ' '}})
		self.table = Table(self.frame, model=self.model, width=w-140, height=h-165,
							rowheight=30, rowheaderwidth=120,
							thefont=('Arial',18))
		self.table.show()
		
		self.cols = [' ']
	
	def change_path(self):
		path = askopenfilename(title='開啟資料庫', filetypes=[('Aoi','*.aoi'),('AoiTEMP','*aoi_temp')])
		self.path_text.set(path)
		
	def load(self):
		path = self.path_text.get()
		if not path:
			return 
			
		if path[-8:]=='aoi_temp':
			self.DB.load_by_temp(path)
		else:
			self.DB.load(path)
	
		for i in self.cols:
			index = self.model.getColumnIndex(i)
			self.model.deleteColumn(index)
		
		for i in range(self.model.getRowCount()):
			self.model.deleteRow(0)
		
		self.cols = copy(self.DB.col())
		data = {str(i.id):{j:str(i[j]) for j in self.cols} for i in self.DB}
		
		self.model.importDict(data)
		self.table.redraw()
		self.table.adjustColumnWidths()
		self.table.redraw()
		
		if path[-8:]=='aoi_temp':
			self.DB.save(path[:-5],True)
			os.remove(path)
	
	def show(self, *args):
		for i in self.cols:
			index = self.model.getColumnIndex(i)
			self.model.deleteColumn(index)
		
		for i in range(self.model.getRowCount()):
			self.model.deleteRow(0)
			
		self.cols = args
		data = {str(i.id):{j:i[j] for j in self.cols} for i in self.DB}
		
		width = (self.w-140)//len(self.cols)
		self.table.cellwidth = width
		
		self.model.importDict(data)
		self.table.redraw()
	
	def main(self):
		self.tk.mainloop()

Viewer = AoiViewer()
Viewer.main()
