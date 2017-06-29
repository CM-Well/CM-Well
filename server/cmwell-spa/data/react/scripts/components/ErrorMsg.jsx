class ErrorMsg extends React.Component {
    render() {
        let className = `error-msg ${this.props.severity||''}`
        return <div className="error-msg">
            <div className="title">OOPS! Something went wrong.</div>
            <div className="msg">{this.props.children}</div>
            <img width="404" src="/meta/app/react/images/dead.jpg" />
            <hr/>
        </div>
    }
}

ErrorMsg.TryOrElseRenderErrMsg = block => {
    try { 
        return block()
    } catch(e) {
        return <ErrorMsg>{e.message}</ErrorMsg>
    }
}

define([], () => ErrorMsg)


