"""
Write Aquasim input file from HDF input.

@author: moserand
"""

import os
from crosswater.read_config import read_config
from crosswater.routing_model.aqu_sys import VarSys, CompSys, LinkSys, CalcSys

def header(file):
    file.write('\nAQUASIM\nVersion 2.0 (win/mfc)\n\n{AQUASYS}{' )
    
def options(file):
    file.write('{OPTIONS}{{3}{SECANT}{100}{FALSE}}')

def varsys(file,vs):
    file.write('{VARSYS}{')
    file.write(vs.progvar)
    file.write(vs.constvar)
    file.write(vs.reallistvar)
    file.write(vs.statevar)
    file.write(vs.formvar)
    file.write('}')
    
def procsys(file):
    file.write('{PROCSYS}{')
    file.write('}')
    
def compsys(file, cs):
    file.write('{COMPSYS}{')
    file.write(cs.rivcomp)
    file.write('}')

def linksys(file, ls):
    file.write('{LINKSYS}{')
    file.write(ls.advlink)
    file.write('}')
    
def calcsys(file, cas):
    file.write('{CALCSYS}{')
    file.write(cas.calc)
    file.write('}')
        
def ender(file):
    file.write('{FITSYS}{}{NUMPAR}{{2}{1}{1000}{0}{TRUE}{5}{1000}{0.005}}{PLOTSYS}{{PLOTLISOPT}{{1}{4}{TAB}}{PLOTFILOPT}{{2}{A4}{TRUE}{1}{1}{1}{1}{2.5}{2}{4}{10}{8}{8}{8}{TRUE}{TRUE}{FALSE}}{PLOTSCROPT}{{1}{600}{400}{25}{25}{25}{25}{50}{20}{14}{10}{12}}}{STATESYS}{}}')


def write_aqu(config_file):
    """
    """
    config = read_config(config_file)
    aqu_path = config['routing_model']['aqu_output']
    with open(aqu_path, 'w') as aqu:
        header(aqu)
        options(aqu)
        vs = VarSys(config_file)
        varsys(aqu, vs)
        procsys(aqu)
        cs = CompSys(config_file)
        compsys(aqu, cs)
        ls = LinkSys(config_file)
        linksys(aqu, ls)
        cas=CalcSys(config_file)
        calcsys(aqu, cas)
        ender(aqu)
    aqu_edit = aqu_path[0:-4]+'_edit.aqu'
    os.system('convedit {0} {1}'.format(aqu_path, aqu_edit))