.PHONY: ci clean compile format test typecheck

REBAR=rebar3

ci:
	@echo ">>> Type-check erlang (dialyzer)"
	$(REBAR) dialyzer
	@echo ">>> Run erlang tests (eunit)"
	$(REBAR) eunit --cover
	@echo ">>> Check erlang formatting (erlfmt)"
	$(REBAR) fmt --check
	@echo ">>> Check erlang docs (edoc)"
	$(REBAR) edoc
	@echo ">>> Lint rust (clippy)"
	cargo clippy --release -- -Wclippy::all
	@echo ">>> Check rust formatting (rustfmt)"
	cargo fmt --check

clean:
	$(REBAR) clean

compile:
	$(REBAR) compile

format:
	$(REBAR) fmt
	cargo fmt

test:
	$(REBAR) eunit

typecheck:
	$(REBAR) dialyzer

%:
	$(REBAR) $@
