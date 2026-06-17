# Style Standards

## General Code Shape

- When two implementations are functionally equivalent and equally clear, prefer
  the one that produces less code. Do not add indirection, helpers, or ceremony
  unless it improves safety, readability, reuse, or testability.

## Apex Null Handling

- Prefer `?.` and `??` over verbose ternaries or nested null checks.

Bad:

```apex
String id = recordId == null ? null : recordId.toString();
String name = value == null ? fallback : value;
```

Good:

```apex
String id = recordId?.toString();
String name = value ?? fallback;
```

- Do not add defensive null branches for values already guaranteed non-null by
  the surrounding condition, API contract, or runtime context.

Bad:

```apex
if (date != null && now > date) {}
```

Good:

```apex
if (now > date) {}
```

## Apex Coercion

- Prefer natural string coercion in concatenation.

Bad:

```apex
'job:' + String.valueOf(System.now().getTime());
```

Good:

```apex
'job:' + System.now().getTime();
```

- Prefer `Map<Id, ...>` when the key is naturally a Salesforce id.

Bad:

```apex
Map<String, Job__c> jobsById;
jobsById.put(job.Id.toString(), job);
```

Good:

```apex
Map<Id, Job__c> jobsById;
jobsById.put(job.Id, job);
```

- `Id` values are string-compatible in Apex comparisons. Do not cast or
  stringify an `Id` only to compare it with a `String`.

Bad:

```apex
job.Id.toString() == jobId;
```

Good:

```apex
job.Id == jobId;
```

## Impossible Cases

- Prefer sets when comparing one value against 3+ candidates.

Bad:

```apex
val == A || val == B || val == C;
```

Good:

```apex
values.contains(val);
```

## Apex Control Flow

- Always prefer early returns.
- If an `if` guard owns more than half of a method, invert the guard and return
  early.

Bad:

```apex
if (valid) {
    run();
    return result;
}
return fallback;
```

Good:

```apex
if (!valid) {
    return fallback;
}
run();
return result;
```

- Never cover impossible cases just to appear defensive or increase coverage.

Bad:

```apex
ctx?.getTriggerId();
```

Good:

```apex
ctx.getTriggerId();
```

## Apex Class Layout

- Keep class elements in this order:

1. `public static final` constants
2. `public static` fields
3. `public final` fields
4. `public` fields
5. `private static final` constants
6. `private static` fields
7. `private final` fields
8. `private` fields
9. public accessors
10. private accessors
11. public constructors
12. protected constructors
13. private constructors
14. public static methods
15. public abstract methods
16. public virtual methods
17. public non-virtual methods
18. protected abstract methods
19. private static methods
20. private virtual methods
21. private non-virtual methods
22. public enums
23. public inner interfaces
24. public abstract classes
25. public classes
26. private enums
27. private inner interfaces
28. private abstract classes
29. private classes

- If a class has both public and private methods, separate them with named
  sections. Prefer specific public section names such as `Queries`, `DMLs`,
  `Accessors`, or `Commands`. Use `API` only when no clearer name fits.
  Private methods belong in `Helpers`.

- Repository classes must use `inherited sharing`. Access levels control
  CRUD/FLS enforcement; they do not replace the repository sharing boundary.

```apex
// <editor-fold desc="Queries">
// </editor-fold>

// <editor-fold desc="Helpers">
// </editor-fold>
```
