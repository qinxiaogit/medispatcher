default: before-make
	CGO_ENABLED=0 go build -installsuffix cgo -a -ldflags "-w" -o build/medispatcher

zippack: default
	cp -r Docs/etc build/
	mkdir -p build/usr/local/sbin/
	mv build/medispatcher build/usr/local/sbin/
	cd build && zip -r -v -9 medispatcher * && rm -rf etc usr
before-make:
	@if test ! -e build; then\
		mkdir build;\
	fi

clean:
	rm -rf build