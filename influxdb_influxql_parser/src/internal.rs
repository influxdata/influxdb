//! Internal result and error types used to build InfluxQL parsers
//!
use nom::error::{ErrorKind as NomErrorKind, ParseError as NomParseError};
use nom::Parser;
use std::borrow::Borrow;
use std::fmt::{Display, Formatter};

/// This trait must be implemented in order to use the [`map_fail`] and
/// [`expect`] functions for generating user-friendly error messages.
pub trait ParseError<'a>: NomParseError<&'a str> + Sized {
    fn from_message(input: &'a str, message: &'static str) -> Self;
}

/// An internal error type used to build InfluxQL parsers.
#[derive(Debug, PartialEq, Eq)]
pub enum Error<I> {
    Syntax { input: I, message: &'static str },
    Nom(I, NomErrorKind),
}

impl<I: Display> Display for Error<I> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Syntax { input: _, message } => {
                write!(f, "Syntax error: {}", message)
            }
            Self::Nom(_, kind) => write!(f, "nom error: {:?}", kind),
        }
    }
}

impl<'a> ParseError<'a> for Error<&'a str> {
    fn from_message(input: &'a str, message: &'static str) -> Self {
        Self::Syntax { input, message }
    }
}

/// Applies a function returning a [`ParseResult`] over the result of the `parser`.
/// If the parser returns an error, the result will be mapped to an unrecoverable
/// [`nom::Err::Failure`] with the specified `message` for additional context.
pub fn map_fail<'a, O1, O2, E: ParseError<'a>, E2, F, G>(
    message: &'static str,
    mut parser: F,
    mut f: G,
) -> impl FnMut(&'a str) -> ParseResult<&'a str, O2, E>
where
    F: Parser<&'a str, O1, E>,
    G: FnMut(O1) -> Result<O2, E2>,
{
    move |input| {
        let (input, o1) = parser.parse(input)?;
        match f(o1) {
            Ok(o2) => Ok((input, o2)),
            Err(_) => Err(nom::Err::Failure(E::from_message(input, message))),
        }
    }
}

/// Applies a function returning a [`ParseResult`] over the result of the `parser`.
/// If the parser returns an error, the result will be mapped to a recoverable
/// [`nom::Err::Error`] with the specified `message` for additional context.
pub fn map_error<'a, O1, O2, E: ParseError<'a>, E2, F, G>(
    message: &'static str,
    mut parser: F,
    mut f: G,
) -> impl FnMut(&'a str) -> ParseResult<&'a str, O2, E>
where
    F: Parser<&'a str, O1, E>,
    G: FnMut(O1) -> Result<O2, E2>,
{
    move |input| {
        let (input, o1) = parser.parse(input)?;
        match f(o1) {
            Ok(o2) => Ok((input, o2)),
            Err(_) => Err(nom::Err::Error(E::from_message(input, message))),
        }
    }
}

/// Transforms a [`nom::Err::Error`] to a [`nom::Err::Failure`] using `message` for additional
/// context.
pub fn expect<'a, E: ParseError<'a>, F, O>(
    message: &'static str,
    mut f: F,
) -> impl FnMut(&'a str) -> ParseResult<&'a str, O, E>
where
    F: Parser<&'a str, O, E>,
{
    move |i| match f.parse(i) {
        Ok(o) => Ok(o),
        Err(nom::Err::Incomplete(i)) => Err(nom::Err::Incomplete(i)),
        Err(nom::Err::Error(_)) => Err(nom::Err::Failure(E::from_message(i, message))),
        Err(nom::Err::Failure(e)) => Err(nom::Err::Failure(e)),
    }
}

/// Returns the result of `f` if it satisfies `is_valid`; otherwise,
/// returns an error using the specified `message`.
pub fn verify<'a, O1, O2, E: ParseError<'a>, F, G>(
    message: &'static str,
    mut f: F,
    is_valid: G,
) -> impl FnMut(&'a str) -> ParseResult<&'a str, O1, E>
where
    F: Parser<&'a str, O1, E>,
    G: Fn(&O2) -> bool,
    O1: Borrow<O2>,
    O2: ?Sized,
{
    move |i: &str| {
        let (remain, o) = f.parse(i)?;

        if is_valid(o.borrow()) {
            Ok((remain, o))
        } else {
            Err(nom::Err::Failure(E::from_message(i, message)))
        }
    }
}

impl<I> NomParseError<I> for Error<I> {
    fn from_error_kind(input: I, kind: NomErrorKind) -> Self {
        Self::Nom(input, kind)
    }

    fn append(_: I, _: NomErrorKind, other: Self) -> Self {
        other
    }
}

/// ParseResult is a type alias for [`nom::IResult`] used by nom combinator
/// functions for parsing InfluxQL.
pub(crate) type ParseResult<I, T, E = Error<I>> = nom::IResult<I, T, E>;
