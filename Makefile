all:
	@echo "#!/bin/bash\n\npython3 src/node.py \"\044@\"" > node; chmod +x node

clean:
	rm node