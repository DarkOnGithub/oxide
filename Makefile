.PHONY: build release install create_dataset download_silesia clean

build release:
	cargo build --release

install: release
	install -Dm755 target/release/oxide $(HOME)/.cargo/bin/oxide

create_dataset:
	python scripts/create_dataset.py
	python scripts/convert_to_raw.py
download_silesia:
	python scripts/download_silesia.py
clean:
	find . -type f \( -name "*.oxz" -o -name "*.lz4" \) -delete
	rm -rf temp
	mkdir temp
