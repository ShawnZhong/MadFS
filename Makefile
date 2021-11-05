.PHONY: debug release clean

debug release:
	cmake -S . -B build-$@ -DCMAKE_BUILD_TYPE=$@
	cmake --build build-$@ -j -- --quiet

clean:
	rm -rf build-*
