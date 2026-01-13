.PHONY: ci clean compile doc format test typecheck

REBAR=rebar3

ci:
	@echo ">>> Type-check erlang (dialyzer)"
	$(REBAR) dialyzer
	@echo ">>> Run erlang tests (eunit)"
	$(REBAR) eunit --cover
	@echo ">>> Check erlang formatting (erlfmt)"
	$(REBAR) fmt --check
	@echo ">>> Check erlang docs (ex_doc)"
	$(REBAR) ex_doc
	@echo ">>> Lint rust (clippy)"
	cargo clippy --release -- -Wclippy::all
	@echo ">>> Check rust formatting (rustfmt)"
	cargo fmt --check -- --config imports_granularity=crate

clean:
	$(REBAR) clean

compile:
	$(REBAR) compile

format:
	$(REBAR) fmt
	cargo fmt -- --config imports_granularity=crate

test:
	$(REBAR) eunit
	cargo test

typecheck:
	$(REBAR) dialyzer
	cargo clippy --release -- -Wclippy::all

doc:
	$(REBAR) ex_doc

%:
	$(REBAR) $@
