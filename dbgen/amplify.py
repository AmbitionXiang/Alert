import re
import sys

def process_tbl_file(input_filename, output_filename):
    """
    process .tbl files: Multiply all numbers with two decimal places by 100 (convert to integers).
    
    params:
        input_filename: input .tbl path
        output_filename: output .tbl path
    """
    pattern = r'-?\d+\.\d{2}$'
    
    with open(input_filename, 'r') as infile, open(output_filename, 'w') as outfile:
        for line in infile:
            parts = line.strip().split('|')
            new_parts = []
            
            for part in parts:
                if re.fullmatch(pattern, part):
                    try:
                        num = float(part)
                        new_num = int(num * 100)
                        new_parts.append(str(new_num))
                    except:
                        new_parts.append(part)
                else:
                    new_parts.append(part)
            
            new_line = '|'.join(new_parts) + '\n'
            outfile.write(new_line)

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("usage: python process_tbl.py <input.tbl> <output.tbl>")
        print("example: python process_tbl.py input.tbl output.tbl")
        sys.exit(1)
    
    input_file = sys.argv[1]
    output_file = sys.argv[2]
    
    print(f"input file: {input_file}")
    print(f"output file: {output_file}")
    
    try:
        process_tbl_file(input_file, output_file)
        print(f"Process finished! Results are saved at: {output_file}")
    except Exception as e:
        print(f"Process failed: {e}")
        sys.exit(1)
