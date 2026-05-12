#!/usr/bin/env bash
get_json_val() {
    python3 -c "import sys, json; \
      obj=json.load(open(sys.argv[1])); \
      keys=sys.argv[2].strip('.').split('.'); \
      val=obj; \
      for k in keys: val=val.get(k, 'n/a') if isinstance(val, dict) else 'n/a'; \
      print(val if val is not None else 'n/a')" "$1" "$2" 2>/dev/null || echo "n/a"
}
get_json_val "$1" "$2"
