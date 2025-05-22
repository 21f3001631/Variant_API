import pandas as pd
from io import StringIO
import argparse
from sqlalchemy import create_engine, Engine, select, update
from sqlalchemy.orm import Session
from sqlalchemy.dialects.sqlite import insert as sqlite_upsert
from _dbhelper import Files, Records, init_db
from rich import print as richprint
from rich.prompt import Prompt, IntPrompt
from rich.table import Table
from tqdm.rich import tqdm
from tqdm import TqdmExperimentalWarning
import os
import time
import glob
import warnings
import toml
import shutil


warnings.filterwarnings('ignore', category=TqdmExperimentalWarning)

def trim_file(fname: str) -> pd.DataFrame:
	with open(fname) as f:
		while line := f.readline():
			if line.startswith('##'):
				continue
			if not line.startswith('#CHROM	POS	ID	REF	ALT'):
				raise IDNotFoundException(
					f'The given file {fname} does not appear to have a header row (starting with #CHROM, POS, ...). '
					+ 'This program is trying to look for the ID as the last column header.'
				)
			# \n is already included in line, no need to add again
			text = line + f.read()
			break
	fulldata = pd.read_csv(StringIO(text), sep='\t')
	fid = fulldata.columns[-1]
	assert isinstance(fid, str)
	data = fulldata.loc[:, ['#CHROM', 'POS', 'REF', 'ALT', fid]]
	data.rename(columns={fid: 'IDF'}, inplace=True)
	data.loc[:, 'IDF'] = data.IDF.str.split(':').str[0]
	# data.set_index(['#CHROM', 'POS'], inplace=True)
	data.loc[:, 'file_id'] = fid
	return data


class IDNotFoundException(Exception):
	pass


def get_file_id(fname: str) -> str:
	with open(fname) as f:
		while line := f.readline():
			if line.startswith('##'):
				continue
			if not line.startswith('#CHROM	POS	ID	REF	ALT'):
				break
			file_id = line.rstrip().split('\t')[-1]
			return file_id
	raise IDNotFoundException(
		f'The given file {fname} does not appear to have a header row (starting with #CHROM, POS, ...). '
		+ 'This program is trying to look for the ID as the last column header.'
	)


def add_file(engine: Engine, fname: str, alembic_version: str) -> bool:
	from _dbhelper import create_upsert
	with Session(engine) as sesh:
		file_id = get_file_id(fname)
		# richprint(f'[underline]DEBUG:[/] {fname}: {file_id}[pink][/]')
		file_seen = sesh.get(Files, file_id)
	if file_seen is not None:
		if file_seen.version == alembic_version:
			# TODO: instead of printing message for every file, accumulate a list
			richprint(f'[medium_purple1]File {fname} has an id [bold]{file_id}[/] that is already in the database, skipping.[/]')
			richprint('[italic]This file will not be moved to data/processed to prevent accidental deletion.[/]')
			return False
		# richprint(f'Updating file {fname}')
		with Session(engine) as sesh:
			contents = trim_file(fname)
			# Passing Session here and not the engine because we want this to be in the same commit as adding the file.
			# added = contents.to_sql('Records', sesh.connection(), if_exists='append', index=False, method=create_upsert)
			ids = sesh.scalars(select(Records.record_id).where(Records.file_id == file_id)).all()
			# print(ids)
			contents['record_id'] = ids
			added = sesh.execute(
				update(Records),
				# [d[1].to_dict() for d in contents.iterrows()]
				contents.to_dict(orient='records')
			)
			if added is None or added == 0:
				# some error
				richprint(f'[red]Unkown error while adding contents of {fname}, sql engine reported that no rows were added.[/]')
				return False
			file_seen.version = alembic_version
			sesh.commit()
			shutil.move(fname, 'data/processed/')
		return True
	#! actually add file
	with Session(engine) as sesh:
		contents = trim_file(fname)
		# Passing Session here and not the engine because we want this to be in the same commit as adding the file.
		added = contents.to_sql('Records', sesh.connection(), if_exists='append', index=False)
		if added is None or added == 0:
			# some error
			richprint(f'[red]Unkown error while adding contents of {fname}, sql engine reported that no rows were added.[/]')
			return False
		f = Files(file_id=file_id, file_name=fname, version=alembic_version)
		sesh.add(f)
		sesh.commit()
		f_last = os.path.basename(fname)
		os.rename(f'data/raw/{f_last}', f'data/processed/{f_last}')
	return True


def main():
	try:
		with open('pyproject.toml') as f:
			VERSION = toml.load(f)['project']['version']
	except FileNotFoundError:
		# did not ship pyproject.toml with first version
		VERSION = '1.0.0'
	richprint('[deep_pink3]' + f' vcfdb - version {VERSION} '.center(os.get_terminal_size()[0], '-') + '[/]')
	engine = create_engine('sqlite:///database.sqlite')
	init_db(engine)

	parser = argparse.ArgumentParser(epilog='By default, all files in the data/raw/ folder will be processed.')

	choices=['add', 'find', 'update'],
	subparsers = parser.add_subparsers(
		dest='mode', 
		help='Choose which mode to operate in.',
		required=True
	)
	subparsers.add_parser('add', help='Add VCF files from the data/raw/ folder.')
	subparsers.add_parser('find', help='Find occurences of a (chrom#, position) pair across all seen data.')
	up_subparser = subparsers.add_parser('update', help='Update the vcfdb application with a newer version (provided as .zip).')

	up_subparser.add_argument(
		'update_from',
		type=str,
		help='The (path to the) zip file to update from, only read when `mode` == `update`.',
	)
	
	args = parser.parse_args()

	# if args.file is not None:
	# 	fname = args.file
	# 	richprint(f'Found file [deep_pink3]{fname}[/], only processing [green]1 file[/]')
	# 	add_file(engine, fname)
	# 	return
	try:
		from _dbhelper import get_current_version
		alembic_version = get_current_version()
	except:
		alembic_version = 'None'

	if args.mode == 'add':
		files = glob.glob('data/raw/*.vcf')
		richprint(f'Found VCF files in [cyan]data/raw[/], processing [green]{len(files)} files[/].')
		richprint('[italic]Sub-folders inside [cyan]data/raw[/] will not be processed.[/]')
		start = time.perf_counter()
		for fname in tqdm(files, desc='VCF files processed', unit='files'):
			add_file(engine, fname, alembic_version)
		richprint(f"[cyan]Processing took {time.perf_counter() - start}s[/]")
	elif args.mode == 'find':
		chrom = Prompt.ask('Enter [cyan]chromosome name[/] (e.g. [cyan bold]chr1[/])')
		pos = IntPrompt.ask('Enter [cyan]position[/] (integer)')
		with Session(engine) as sesh:
			results = sesh.scalars(select(Records).filter_by(CHROM=chrom, POS=pos)).all()
			total_files = sesh.query(Files).count()

			tab = Table(title=f'Found [bold deep_pink3]{len(results)} occurences[/] across {total_files} seen files')
			# tab.add_column('#occurences', justify='right', style='cyan')
			tab.add_column('S.No.', style='grey50', justify='right')
			tab.add_column('file_id', style='purple', no_wrap=True)
			tab.add_column('REF', justify='center', style='cyan')
			tab.add_column('ALT', justify='center', style='cyan')
			tab.add_column('#CHROM', style='grey50', overflow='ellipsis')
			tab.add_column('POS', style='grey50', overflow='ellipsis')
			tab.add_column('IDF', style='grey50', overflow='ellipsis')

			for i, r in enumerate(results):
				tab.add_row(str(i), r.file_id, r.REF, r.ALT, r.CHROM, str(r.POS))

			richprint(tab)
	elif args.mode == 'update':
		assert args.update_from is not None, "For updating please provide path to zip file as last argument."
		import zipfile
		reqd_files = {'_dbhelper.py', 'vcfdb.py', 'pyproject.toml', 'alembic/', 'alembic.ini'}
		with zipfile.ZipFile(args.update_from, 'r') as zip_ref:
			names = zip_ref.namelist()
			# print(names)
			assert len(reqd_files.difference(names)) == 0, f"One or more file(s) missing from zip archive {args.update_from}.\n{reqd_files.difference(names)}"
		
			for file in names:
				zip_ref.extract(file, 'temp_update')

		with open('temp_update/pyproject.toml') as f:
			temp_version = toml.load(f)['project']['version']

		def versiontuple(v):
			return tuple(map(int, (v.split("."))))

		assert versiontuple(temp_version) > versiontuple(VERSION), "Package already updated/trying to update with a lower version."

		for file in names:
			if (folder := os.path.dirname(file)) != '':
				os.makedirs(folder, exist_ok=True)
			# print(f'{file}, inside folder {folder}')
			if os.path.isdir(file):
				# a folder itself, can't copy
				# print(f'ISFOLDER: {file}')
				continue
			shutil.copy(f'temp_update/{file}', file)

		shutil.rmtree('temp_update')

		# Use the updated _dbhelper to apply migrations
		import importlib
		import _dbhelper
		importlib.reload(_dbhelper)
	
		_dbhelper.apply_migrations()

		for file in glob.glob('data/processed/*.vcf'):
			shutil.move(file, 'data/raw/')
		richprint('[cyan]Update and migrations completed. Processed files have been moved to raw folder.[/]')
		richprint('[red][bold]Please run the add command[/bold] to re-process files to fill any databases fields added by the update.[/]')


if __name__ == '__main__':
	main()
