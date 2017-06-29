define((require, exports, module) => {

let Components = module.config().components
    
let { Router, Route, Link, browserHistory } = ReactRouter
let { Header, SearchBar, Breadcrumbs, HomePage, InfotonsList, Infoton, Footer } = Components

class App extends React.Component {
    
    // TODO Common ajaxes should be in App.state, propageted to children by props.
    // TODO e.g. /?op=stream should be <SearchBar rootFolders={this.state.rootFolders}/>
    
    constructor(props) {
        super(props)
        this.state = {
            currentHasChildren: true
        }
        
        if(!this._getInjectedInfoton())
            browserHistory.push(location.search && /\?path=(.*)/.exec(location.search)[1] || '/')
    }
    
    componentDidMount() {
        $('#content').show()
        $('.spinner-container, #loading-status').fadeOut(250)
    }
    
    render() {
        AppUtils.debug('App.render')

        let injectedInfoton = this._getInjectedInfoton()
        
        return (
            <div id="app-container">
                <Header/>
                <SearchBar currentHasChildren={this.state.currentHasChildren} />
                <Breadcrumbs/>
                <InfotonsList location={this.props.location} isRoot={true} hasChildrenCb={this.toggleState.bind(this, 'currentHasChildren')} />
                <Infoton location={this.props.location} infoton={injectedInfoton} />
                <Footer/>
            </div>
        )
    }
    
    _getInjectedInfoton() {
        let injectedInfoton = document.getElementsByTagName('inject')[0] && JSON.parse(document.getElementsByTagName('inject')[0].innerHTML)
        return injectedInfoton && new Domain.Infoton(JSON.fromJSONL(injectedInfoton))
    }
}

ReactDOM.render((
  <Router history={browserHistory}>
    <Route path="*" component={App}/>
  </Router>
), document.getElementById('content'))

})
