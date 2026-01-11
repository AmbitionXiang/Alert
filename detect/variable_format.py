import re

def replace_variable_format(s, factor):
    """
    convert 'x<prefix>_<suffix>' to 'x' + (prefix*1000 + suffix)
    
    examples:
        "x12_789" -> "x12789"   (12*1000 + 789 = 12789)
        "x1_2"    -> "x1002"    (1*1000 + 2 = 1002)
        "x100_5"  -> "x100005"  (100*1000 + 5 = 100005)
    """
    def replace_func(match):
        prefix = match.group(1)
        suffix = match.group(2)
        new_value = int(prefix) * factor + int(suffix)
        return f'x{new_value}'
    
    return re.sub(r'x(\d+)_(\d+)', replace_func, s)

if __name__ == "__main__":
    test_strings = [
        "x12_789 + x1_2 = 5",
        "x100_5 * x123_456",
        "x_123",
        "x123_",
        "xabc_123",
        "x12_345_678",
        "x12_789x12_789",
        "x12_789 and x1_2",
        "x12_789 is 12789",
        "x12_789 + x12_789",
    ]
    
    for s in test_strings:
        result = replace_variable_format(s, 10000)
        print(f"source: {s}")
        print(f"target: {result}\n")