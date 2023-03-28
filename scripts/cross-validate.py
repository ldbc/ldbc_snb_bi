import argparse
from pathlib import Path
from recursive_diff import recursive_diff
from more_itertools import peekable
import json
import textwrap
from collections import Counter

parser = argparse.ArgumentParser()
parser.add_argument('--scale-factor', type=str, help='Scale Factor', required=True)
parser.add_argument('--tool-expected', type=str, help='The name of the tool producting the expected output', required=True)
parser.add_argument('--tool-actual', type=str, help='The name of tool producing the actual output', required=True)
parser.add_argument('--output-expected', type=Path, help='The output path of the expected output', required=True)
parser.add_argument('--output-actual', type=Path, help='The output path of the actual output', required=True)
parser.add_argument('--verbose', action='store_true', help='Verbose mode: print outputs of both tools if results are different')
args = parser.parse_args()

number_of_query_instances = Counter()
validation_errors = Counter()
skipped_lines = Counter()
misaligned_parameters = Counter()

print("========== Validation errors ===========")

with open(args.output_expected) as outputfile_expected, open(args.output_actual) as outputfile_actual:
    lines_expected = outputfile_expected.readlines()
    lines_actual = outputfile_actual.readlines()
    
    i = 0
    j = 0
    num_skipped = 0
    query_variant = 0
    num_misaligned_parameters = 0
    while i < len(lines_expected) and j < len(lines_actual):
        line_expectedsplit = lines_expected[i].strip().split("|")
        line_actualsplit = lines_actual[j].strip().split("|")

        (query_expected, query_variant_expected, parameters_expected) = tuple(line_expectedsplit[0:3])
        result_expected = json.loads(line_expectedsplit[3])
        
        (query_actual, query_variant_actual, parameters_actual) = tuple(line_actualsplit[0:3])
        result_actual = json.loads(line_actualsplit[3])

        # checking whether the query variants align in the two inputs
        if query_variant_expected > query_variant_actual:
            j = j + 1
            num_skipped = num_skipped + 1
            continue

        # we can save the query variant, the number of num_skipped lines, and advance to the next line
        query_variant = query_variant_expected

        if num_skipped > 0:
            skipped_lines[query_variant] = num_skipped
            print(f"* Q{query_variant_actual}: The output of tool '{args.tool_expected}' has {num_skipped} fewer entry/entries than tool '{args.tool_actual}'.")
            num_skipped = 0

        i = i + 1
        j = j + 1

        # checking whether the parameters align for the same query variant in the current row
        if parameters_expected != parameters_actual:
            num_misaligned_parameters = num_misaligned_parameters + 1
            continue

        if num_misaligned_parameters > 0:
            misaligned_parameters[query_variant] = num_misaligned_parameters
            num_misaligned_parameters = 0
            print(f"* Q{query_variant}: {misaligned_parameters[query_variant]} parameters were different, these were skipped.")

        parameters = parameters_expected

        diff = recursive_diff(result_expected, result_actual, abs_tol=0.00001)
        diff = peekable(diff)

        number_of_query_instances[query_variant] += 1
        if diff:
            print(textwrap.dedent(f"""
                * Q{query_variant}: Outputs differ for inputs {parameters}:
                """).strip())

            for d in diff:
                print(f"  - {d}")

            if args.verbose:
                print(textwrap.dedent(f"""
                Output of tool '{args.tool_expected}':
                {result_expected}

                Output of tool '{args.tool_actual}':
                {result_actual}
                """).strip())
                print()

            validation_errors[query_variant] += 1

print()
print("========== Validation summary ==========")

if skipped_lines:
    print(f"Skipped a total of {sum(skipped_lines.values())} line(s):")
    for q, num in skipped_lines.items():
        print(f"- Q{q}: {num} lines skipped")
    print()

if misaligned_parameters:
    print(f"Found {sum(misaligned_parameters.values())} misaligned input parameters:")
    for q, num in misaligned_parameters.items():
        total = number_of_query_instances[q]
        print(f"- Q{q}: {num} parameters misaligned out of {total} query instances")
    print()

if validation_errors:
    print(f"Found {sum(validation_errors.values())} validation error(s):")
    for q, num in validation_errors.items():
        total = number_of_query_instances[q]
        print(f"- Q{q}: {num} failed out of {total} query instances")
    print()

if skipped_lines or validation_errors:
    print("Validation failed.")
    exit(-1)

print("Validation passed.")
