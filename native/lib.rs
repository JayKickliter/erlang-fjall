use rustler::Atom;

mod atom {
    rustler::atoms! {
        ok,
        error,
    }
}

#[rustler::nif]
fn hello() -> Atom {
    atom::ok()
}

rustler::init!("fjall");
