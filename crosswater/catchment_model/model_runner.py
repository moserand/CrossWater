"""Run all catchment models.

Calls external executable.
"""

import os
from pathlib import Path
from queue import Queue
import shutil
import subprocess
import time
from threading import Thread
import sys

import pandas
import tables

from crosswater.read_config import read_config
from crosswater.tools.hdf5_helpers import find_ids
from crosswater.tools.path_helper import ChDir
from crosswater.tools.time_helper import ProgressDisplay


class ModelRunner(object):
    """Run all catchment models"""
    # pylint: disable=too-few-public-methods,too-many-instance-attributes
    def __init__(self, config_file):
        """
        """
        config = read_config(config_file)
        self.debug = False
        debug = config['catchment_model']['debug'].strip().lower()
        if debug in ['true', 'yes', 'y']:
            self.debug = True
        self.input_file_name = config['catchment_model']['hdf_input_path']
        self.output_file_name = config['catchment_model']['output_path']
        self.tmp_path = Path(config['catchment_model']['tmp_path'])
        self.layout_xml_path = Path(
            config['catchment_model']['layout_xml_path'])
        self.number_of_workers = config['catchment_model']['number_of_workers']
        self.program_path = Path(__file__).parents[2] / Path('program')
        self._make_template_name()
        self._open_files()
        self._prepare_tmp()
        self.output_table = None
        self._init_hdf_output()
        self.queue = Queue()
        self.use_wine = False
        if sys.platform != 'win32':
            self.use_wine = True

    def _make_template_name(self):
        """Create template for the name of the layout file.
        """
        self.layout_name_template = '{stem}_{{id}}{suffix}'.format(
            stem=self.layout_xml_path.stem,
            suffix=self.layout_xml_path.suffix)

    def _prepare_tmp(self):
        """Create tmp path and worker paths. Copy executable.
        """
        if self.tmp_path.exists():
            shutil.rmtree(str(self.tmp_path))
        self.worker_paths = []
        shutil.copytree(str(self.program_path), str(self.tmp_path))
        for worker in range(self.number_of_workers):
            worker_path = self.tmp_path / Path('worker_{}'.format(worker))
            worker_path.mkdir()
            self.worker_paths.append(worker_path)

    def _open_files(self):
        """Open HDF5 input and output files.
        """
        self.hdf_input = tables.open_file(self.input_file_name, mode='r')
        self.hdf_output = tables.open_file(self.output_file_name, mode='w',
                                           title='Crosswater results')

    def _init_hdf_output(self):
        """Create empty output tables with timestep, ID, Q, and C.
        """
        filters = tables.Filters(complevel=5, complib='zlib')
        self.output_table = self.hdf_output.create_table(
            '/', 'output', OutputValues, filters=filters)
        self.output_table.flush()

    def _close_files(self):
        """Close HDF5 input and output files.
        """
        self.hdf_input.close()
        self.hdf_output.close()

    @staticmethod
    def _read_parameters(group):
        """Read all parameters.
        """
        # pylint: disable=protected-access
        parameters_table = group._f_get_child('parameters')
        return {row['name'].decode('ascii'): row['value'] for row in
                parameters_table}

    @staticmethod
    def _read_inputs(group):
        """Read whole input table.
        """
        # pylint: disable=protected-access
        parameters_table = group._f_get_child('inputs')
        return parameters_table[:]

    def _read_parameters_inputs(self, id_):
        """Read parameters and inputs for gibe ID.
        """
        group = self.hdf_input.get_node('/', 'catch_{}'.format(id_))
        return self._read_parameters(group), self._read_inputs(group)

    def _write_output(self, id_, out):
        """Write out from one run to HDF5 output file.
        """
        row = self.output_table.row
        zipped = zip(out['discharge'], out['concentration'])
        for step, (discharge, concentration) in enumerate(zipped, 1):
            row['timestep'] = step
            row['catchment'] = id_
            row['discharge'] = discharge
            row['concentration'] = concentration
            row.append()

    def _run_all(self):
        """Run all models with out catching all exceptions.
        The similary named `run_all()` method will close all HDF5 files
        after an exception in this method.
        """
        all_ids = find_ids(self.hdf_input)
        nids = len(all_ids)
        prog = ProgressDisplay(nids)
        all_ids = iter(all_ids)
        free_paths = self.worker_paths[:]
        active_workers = {}
        done = False
        counter = 0
        with ChDir(str(self.tmp_path)):
            while True:
                for path in free_paths:
                    try:
                        id_ = next(all_ids)
                    except StopIteration:
                        done = True
                        break
                    counter += 1
                    prog.show_progress(counter, additional=id_)
                    parameters, inputs = self._read_parameters_inputs(id_)
                    worker = Worker(id_, path, parameters, inputs,
                                    self.layout_xml_path,
                                    self.layout_name_template,
                                    self.queue,
                                    debug=self.debug,
                                    use_wine=self.use_wine)
                    worker.start()
                    active_workers[path] = worker

                free_paths = []
                for path, worker in active_workers.items():
                    if not worker.is_alive():
                        free_paths.append(path)
                while not self.queue.empty():
                    self._write_output(*self.queue.get())
                if done:
                    break
            for worker in active_workers.values():
                worker.join()
            while not self.queue.empty():
                self._write_output(*self.queue.get())
        prog.show_progress(counter, additional=id_, force=True)
        print()

    def run_all(self):
        """Run all models.
        """
        try:
            self._run_all()
        finally:
            self._close_files()


class Worker(Thread):
    """One model run.
    """
    def __init__(self, id_, path, parameters, inputs, layout_xml_path,
                 layout_name_template, queue, debug=False, use_wine=False):
        # pylint: disable=too-many-arguments
        super().__init__()
        self.id = id_
        self.path = path
        self.parameters = parameters
        self.inputs = inputs
        self.layout_xml_path = layout_xml_path
        self.layout_name_template = layout_name_template
        self.queue = queue
        self.debug = debug
        self.use_wine = use_wine
        self.output_path = self.path / 'out_{}.txt'.format(self.id)
        self.input_path = self.path / self.layout_name_template.format(
            id=self.id)
        self.txt_input_path = self.path / Path('input_{}.txt'.format(self.id))
        self.daemaon = True

    def _make_input(self):
        """Create input files.
        """
        self._make_layout(self.parameters)
        self._make_time_varying_input(self.inputs)

    def _make_layout(self, parameters):
        """Create the XML file for the model layout.
        """
        # pylint: disable=star-args
        with open(str(self.layout_xml_path))as fobj:
            layout_template = fobj.read()
        layout = layout_template.format(id=self.id,
                                        input_file_name=self.txt_input_path,
                                        **parameters)
        layout_file_name = Path(self.layout_name_template.format(id=self.id))
        model_layout_path = self.path / layout_file_name
        with open(str(model_layout_path), 'w') as fobj:
            fobj.write(layout)

    def _make_time_varying_input(self, inputs):
        """Create the text file for the input of T, P, an Q.
        """
        with open(str(self.txt_input_path), 'w') as fobj:
            fobj.write('step\tT\tP\tQ\tEmptymeas\n')
            for step, row in enumerate(inputs):
                fobj.write('{step}\t{T}\t{P}\t{Q}\tN/A\n'.format(
                    step=step, T=row['temperature'], P=row['precipitation'],
                    Q=row['discharge']))

    def _execute(self):
        """Run external program for catchment model.
        """
        cmd_list = ['server.exe', str(self.input_path), 'RUN',
                    str(self.output_path)]
        shell = True
        if self.use_wine:
            cmd_list = ['wine'] + cmd_list
            shell = False
        try:
            _ = subprocess.check_output(cmd_list, stderr=subprocess.STDOUT,
                                        shell=shell)
        except subprocess.CalledProcessError as err:
            print('error running model for ID {} with worker {}'.format(
                  self.id, self.path))
            print('exit status:', err.returncode)
            print('err:', err)
            print()

    def run(self):
        """Run thread.
        """
        self._make_input()
        self._execute()
        self._read_output()

    def _read_output(self):
        """Read output from catchment model.
        """
        usecols = ['Q', 'CalcC_atrazin_{}'.format(self.id)]
        out = pandas.read_csv(str(self.output_path), delim_whitespace=True,
                              usecols=usecols)
        out.columns = pandas.Index(['discharge', 'concentration'])
        self.queue.put((self.id, out))
        if not self.debug:
            for path in [self.input_path, self.txt_input_path,
                         self.output_path]:
                try:
                    os.remove(str(path))
                # External progam may block. Try several times with timeout.
                except PermissionError:
                    wait = 1
                    step = 0.1
                    while wait > 0:
                        wait -= step
                        time.sleep(step)
                        try:
                            os.remove(str(path))
                            break
                        except PermissionError:
                            pass


class OutputValues(tables.IsDescription):
    # pylint: disable=too-few-public-methods
    """Data model for output data table.
    """
    timestep = tables.Int32Col()
    catchment = tables.StringCol(10)
    discharge = tables.Float64Col()
    concentration = tables.Float64Col()
