# Deleting Known Field Values #

----

**Go to:** &nbsp;&nbsp;&nbsp;&nbsp; [**Root TOC**](CM-Well.RootTOC.md) &nbsp;&nbsp;&nbsp;&nbsp; [**Topic TOC**](DevGuide.BestPractices.TOC.md) &nbsp;&nbsp;&nbsp;&nbsp; [**Previous Topic**](DevGuide.BestPractices.StreamingMethods.md)

----

In CM-Well, it's legal for a single field to have more than one value.

Here is an example where **someField** has 2 values:

    <http://example.org/my/infoton> <http://ont.example.org/v1.0/ns#someField> "value#1" .
    <http://example.org/my/infoton> <http://ont.example.org/v1.0/ns#someField> "value#2" .
    
Suppose we want to preserve **value#1** and delete **value#2**.

One way to do it is with the **markReplace** operator:

    <http://example.org/my/infoton> <cmwell://meta/sys#markReplace> <http://ont.example.org/v1.0/ns#someField> .
    <http://example.org/my/infoton> <http://ont.example.org/v1.0/ns#someField> "value#1" .

This command deletes all the values in **someField** and writes **value#1** to **someField**.
(We could also use the **replace-mode** flag, which implicitly adds a **markReplace** statement to every ingested field.) But this method may cause unexpected results.

The other way is to use the **markDelete** operator to specifically delete the unwanted value:

    <http://example.org/my/infoton> <cmwell://meta/sys#markDelete> _:anon .
    _:anon <http://ont.example.org/v1.0/ns#someField> "value#2" .

The reason to use **markDelete**, when you know what value you want to delete, is that you know exactly which values will be deleted. The **markReplace** command deletes all existing fields before writing the new value. Therefore, using this command might delete some values added by another user, which was not the intention of the user calling **markReplace**.

Therefore, to delete one or more specific field values, we recommend using the **markDelete** operator.

----

**Go to:** &nbsp;&nbsp;&nbsp;&nbsp; [**Root TOC**](CM-Well.RootTOC.md) &nbsp;&nbsp;&nbsp;&nbsp; [**Topic TOC**](DevGuide.BestPractices.TOC.md) &nbsp;&nbsp;&nbsp;&nbsp; [**Previous Topic**](DevGuide.BestPractices.StreamingMethods.md)

----