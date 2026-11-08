# ADR 0001: Record architecture decisions

Status: accepted

## Context

This project is small, but most of its value is in the decisions behind it:
what the system needs, what it deliberately does not do, and where it would
change at 100x the load. Code shows the outcome of a decision, not the
alternatives that were rejected.

## Decision

Significant decisions are recorded as short ADRs in `docs/adr`, numbered in
order. Each one states the context, the decision, and its consequences,
including the ones that hurt.

## Consequences

A reviewer can read the reasoning without reverse-engineering it from code.
ADRs are written when the decision is made, not afterwards, so they also
document the order in which the design came together.
