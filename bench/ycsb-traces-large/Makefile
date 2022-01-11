ycsb := ycsb-0.17.0
trace_files := a-load.txt b-load.txt c-load.txt d-load.txt e-load.txt f-load.txt \
               a-run.txt  b-run.txt  c-run.txt  d-run.txt  e-run.txt  f-run.txt

.PHONY: all
all: $(trace_files)
$(trace_files): $(ycsb)
	$(eval parts := $(subst .txt,,$(subst -, ,$@)))
	$(eval w := $(firstword $(parts)))
	$(eval t := $(lastword $(parts)))
	$(ycsb)/bin/ycsb.sh $(t) basic \
		-P $(ycsb)/workloads/workload$(w) \
		-p fieldcount=1 -p fieldlength=0 \
		-p recordcount=100000 -p operationcount=100000 \
		> $(w)-$(t).txt

$(ycsb):
	curl -O --location https://github.com/brianfrankcooper/YCSB/releases/download/0.17.0/ycsb-0.17.0.tar.gz
	tar xfvz ycsb-0.17.0.tar.gz

.PHONY: clean
clean:
	rm -f *.txt
