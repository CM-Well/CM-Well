let { Link } = ReactRouter

class ErrorMsg extends React.Component {
    render() {
        let className = `error-msg`
        
        let fullErrMsg = this.props.children
        let onlyBody = fullErrMsg.indexOf(':')===-1 ? fullErrMsg : fullErrMsg.substr(fullErrMsg.indexOf(':')+2)
        
        return <div className="error-msg">
            <div className="msg">
                <img id="error-sign" src="/meta/app/react/images/recoverable-error-sign.svg" />
                <div>{onlyBody}</div>
                <img id="cone1" src="/meta/app/react/images/cone-1.svg" />
                <img id="cone2" src="/meta/app/react/images/cone-2.svg" />
            </div>
            <div className="details">
                {fullErrMsg}.
            <br/>
                Go back to <Link to='/'>home page</Link> or search again.
            </div>
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


