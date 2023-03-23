import argparse
from pathlib import Path
from recursive_diff import recursive_diff
from more_itertools import peekable
import json
import textwrap
from collections import Counter

parser = argparse.ArgumentParser()
parser.add_argument('--scale-factor', type=str, help='Scale Factor', required=True)
parser.add_argument('--tool1', type=str, help='The name of tool 1', required=True)
parser.add_argument('--tool2', type=str, help='The name of tool 2', required=True)
parser.add_argument('--output1', type=Path, help='The output path of tool 1', required=True)
parser.add_argument('--output2', type=Path, help='The output path of tool 2', required=True)
parser.add_argument('--verbose', action='store_true', help='Verbose mode: print outputs of both tools if results are different')
args = parser.parse_args()

number_of_query_instances = Counter()
validation_errors = Counter()

with open(args.output1) as outputfile1, open(args.output2) as outputfile2: 
    for line1, line2 in zip(outputfile1, outputfile2):
        line1split = line1.strip().split("|")
        line2split = line2.strip().split("|")
        
        query = line1split[0]
        query_variant = line1split[1]
        parameters = line1split[2]
        result1 = json.loads(line1split[3])
        result2 = json.loads(line2split[3])

        diff = recursive_diff(result1, result2, abs_tol=0.00001)
        diff = peekable(diff)
        
        number_of_query_instances[query_variant] += 1
        if diff:
            print(textwrap.dedent(f"""
                Outputs differ for:
                - Query variant: Q{query_variant}
                - Input parameters: {parameters}
                Difference(s):
                """).strip())

            for d in diff:
                print(f"- {d}")

            if args.verbose:
                print(textwrap.dedent(f"""
                Output of tool '{args.tool1}':
                {result1}

                Output of tool '{args.tool2}':
                {result2}
                """).strip())
            print()

            validation_errors[query_variant] += 1

if validation_errors:
    print(f"Validation failed with validation error(s)")
    for q, num in validation_errors.items():
        total = number_of_query_instances[query_variant]
        print(f"- Q{query_variant}: {num} failed out of {total} query instances")
    exit(-1)
else:
    print("Validation passed")
