import sqlalchemy
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, Session
from sqlalchemy import Engine, ForeignKey, text
import subprocess
import shutil
from pandas.io.sql import SQLTable

class Base(DeclarativeBase):
	pass


class Files(Base):
	__tablename__ = 'Files'
	file_id: Mapped[str] = mapped_column(primary_key=True)
	file_name: Mapped[str] = mapped_column(unique=True)
	version: Mapped[str] = mapped_column(nullable=True)
	# not needed for our case
	# record_ids: Mapped[List[Records]] = relationship(back_populates='Records')


class Records(Base):
	__tablename__ = 'Records'
	record_id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
	CHROM: Mapped[str] = mapped_column(name='#CHROM')
	POS: Mapped[int]
	REF: Mapped[str]
	ALT: Mapped[str]
	IDF: Mapped[str] = mapped_column(nullable=True)
	# file_id: Mapped[Files] = relationship(back_populates='Files')
	file_id = mapped_column(ForeignKey('Files.file_id'))


def init_db(engine: Engine):
	if not sqlalchemy.inspect(engine).has_table('Files'):
		Files.metadata.create_all(engine)
	if not sqlalchemy.inspect(engine).has_table('Records'):
		Records.metadata.create_all(engine)


def get_current_version():
	# ! Only run after check alembic
	result = subprocess.run(['alembic', 'current'], capture_output=True, text=True)
	version = result.stdout
	if version == '':
		version = 'base'
	return version.replace(' ', '_')


def apply_migrations():
	# current_version = get_current_version(session)
	# print(f'Current database version: {current_version}')

	assert check_alembic_installed(), "Please install alembic using `pip install alembic`."

	shutil.copy2('database.sqlite', f'database.sqlite.bak')

	result = subprocess.run(['alembic', 'upgrade', 'head'], capture_output=True, text=True)
	print('Migration log')
	print('stdout:', result.stdout)
	print('stderr:', result.stderr)

	# print(f'Database is now at version: {get_current_version(session)}')


def check_alembic_installed():
	try:
		result = subprocess.run(['alembic', '--version'], capture_output=True, text=True, check=True)
		return True
	except subprocess.CalledProcessError as e:
		print(f"Error: {e}")
		return False
	except FileNotFoundError:
		return False

# Example usage
if __name__ == "__main__":
	print(check_alembic_installed())
