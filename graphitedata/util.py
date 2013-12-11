def is_pattern(s):
   return '*' in s or '?' in s or '[' in s or '{' in s

def is_escaped_pattern(s):
  for symbol in '*?[{':
    i = s.find(symbol)
    if i > 0:
      if s[i-1] == '\\':
        return True
  return False

def find_escaped_pattern_fields(pattern_string):
  pattern_parts = pattern_string.split('.')
  for index,part in enumerate(pattern_parts):
    if is_escaped_pattern(part):
      yield index