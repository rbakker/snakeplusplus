"""An example script, run by the rule Greet: python greet.py GREETING NAME OUTPUT_FILE"""
import sys

greeting, name, output_file = sys.argv[1:]
with open(output_file, 'w') as f:
    f.write(f'{greeting}, {name}!\n')
print(f'Wrote {output_file}')
