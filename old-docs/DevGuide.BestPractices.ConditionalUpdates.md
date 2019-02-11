# Avoiding Write Clashes with Conditional Updates #

----

**Go to:** &nbsp;&nbsp;&nbsp;&nbsp; [**Root TOC**](CM-Well.RootTOC.md) &nbsp;&nbsp;&nbsp;&nbsp; [**Topic TOC**](DevGuide.BestPractices.TOC.md) &nbsp;&nbsp;&nbsp;&nbsp; [**Previous Topic**](DevGuide.BestPractices.TOC.md)&nbsp;&nbsp;&nbsp;&nbsp; [**Next Topic**](DevGuide.BestPractices.ManagingRetries.md)  

----


If your application supports a user interface that enables writing to CM-Well, it's possible for several users to request updates to the same infoton at the same time. This can potentially result in one user's updates silently overwriting another user's updates.

To avoid this situation, you can use the **conditional update** feature. To use the feature, you send the UUID of the infoton you want to update, and if the UUID has changed since the time you last read it, you can show a warning or error to your user.

See [Using Conditional Updates](API.UsingConditionalUpdates.md) to learn more about conditional update syntax.

If your application provides a UI for updating CM-Well, we recommend implementing the following application logic:

1. Read the relevant data from CM-Well before displaying it to the user.
2. Periodically refresh the displayed data.
3. Refresh the data again when entering editing mode.
4. When attempting an update, use the conditional update feature (send the previous UUID for verification).
5. If you receive a status indicating that the infoton has changed since your last read, handle according to your requirements (for instance, fail the update or display a warning).

----

**Go to:** &nbsp;&nbsp;&nbsp;&nbsp; [**Root TOC**](CM-Well.RootTOC.md) &nbsp;&nbsp;&nbsp;&nbsp; [**Topic TOC**](DevGuide.BestPractices.TOC.md) &nbsp;&nbsp;&nbsp;&nbsp; [**Previous Topic**](DevGuide.BestPractices.TOC.md)&nbsp;&nbsp;&nbsp;&nbsp; [**Next Topic**](DevGuide.BestPractices.ManagingRetries.md)  

----