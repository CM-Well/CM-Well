let { Link } = ReactRouter

class HomePage extends React.Component {
    constructor(props) {
        super(props)
        this.state = {
            rootFolders: []
        }
    }
    
    componentWillMount() {
        AppUtils.cachedGet('/?op=stream')
            .then(list => this.setState({ rootFolders: list.split`\n` }))
            .fail(r => this.setState({ errMsg: AppUtils.ajaxErrorToString(r) }))
    }
    
    render() {
        return this.state.errMsg ? <ErrorMsg>{this.state.errMsg}</ErrorMsg> :
            <div className="home-page-grid-container">
                { this.state.rootFolders.map(rf => <FolderBox folder={rf} />) }
            </div>
    }
}

class FolderBox extends React.Component {
    render() {
        return <Link to={this.props.folder} className="folder-box">
            <img src="/meta/app/react/images/folder-box.png" />
            { this.props.folder.substr(1) }
        </Link>
    }
}

define([], () => HomePage)