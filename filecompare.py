import sys
import filecmp

print(filecmp.cmp(sys.argv[1], sys.argv[2]))