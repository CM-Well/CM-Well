define((require, exports, module) => {

let Components = module.config().components
    
let { Router, Route, Link, browserHistory } = ReactRouter
let { Header, SearchBar, Breadcrumbs, HomePage, InfotonsList, Infoton, Footer } = Components

class App extends React.Component {
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
        
        // fetching common meta-data
        AppUtils.fetchDisplayNames(data => this.setState({ displayNames: data }))
        AppUtils.cachedGet('/?op=stream').then(paths => this.setState({ rootFolders: paths.split`\n` }))
    }
    
    render() {
        AppUtils.debug('App.render')

        let injectedInfoton = this._getInjectedInfoton()
        
        return (
            <div id="app-container">
                
                <Header/>
                
                <SearchBar
                    currentHasChildren={this.state.currentHasChildren}
                    rootFolders={this.state.rootFolders}
                />
                
                <Breadcrumbs lastBreadcrumbDisplayName={this.state.lastBreadcrumbDisplayName} />
                
                <InfotonsList
                    location={this.props.location}
                    isRoot={true}
                    hasChildrenCb={hasChildren => this.setState({ currentHasChildren: hasChildren })}
                    displayNames={this.state.displayNames}
                />
                
                <Infoton
                    location={this.props.location}
                    infoton={injectedInfoton}
                    rootFolders={this.state.rootFolders}
                    displayNames={this.state.displayNames}
                    displayNameUpdateCb={dn => dn && this.state.lastBreadcrumbDisplayName!=dn && this.setState({ lastBreadcrumbDisplayName: dn })}
                />
                
                <Footer/>
                
            </div>
        )
    }
    
    _getInjectedInfoton() {
        let injectedInfoton = document.getElementsByTagName('inject')[0] && JSON.parse(document.getElementsByTagName('inject')[0].innerHTML)
        return injectedInfoton && new Domain.Infoton(JSON.fromJSONL(injectedInfoton))
    }
}
    
class TempProc extends React.Component {
    componentDidMount() {
        $('#content').show()
        $('.spinner-container, #loading-status').fadeOut(250)
        
    }

    render() {
        return <div className="temp-proc">
            <b>/proc</b> is not yet implemented in new UI. Stay tuned for updates!<br/>
            <span className="link" onClick={()=>location.href=location.href+'?old-ui'}>Click here to view {location.pathname} in old UI</span>
        </div>
    }
}

ReactDOM.render((
  <Router history={browserHistory}>
    <Route path="/proc" component={TempProc}/>
    <Route path="/proc/:page" component={TempProc}/>
    <Route path="*" component={App}/>
  </Router>
), document.getElementById('content'))

})
