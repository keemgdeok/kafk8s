# Partial CLI verification

Eleven cases passed, but the authorization-denied fixture expected exit 2 after
removing one cluster Describe Allow and received exit 0. Other read permissions
and API behavior still allowed inspection. This does not demonstrate a product
authorization bypass, nor does it count as an overall verification pass.

The next run resets fixture ACLs and uses an explicit topic Describe Deny for
the dedicated lab principal. Original JSON/stderr and per-case results remain
unchanged here.
