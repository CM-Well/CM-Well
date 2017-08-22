class Footer extends React.Component {
    render() {
        AppUtils.debug('Footer.render')
        
        let footerItems = [
//             { title: 'Cookie Policy', href: '#' }
//            ,{ title: 'Privacy Statement', href: '#' }
//            ,{ title: 'Term of Use', href: '#' }
//            ,{ title: 'Copyright', href: '#' }
//            ,{ title: 'Feedback', href: '#' }
        ]

        return <div className="footer">
                    <a href="http://www.thomsonreuters.com" target="_blank">
                        <img src="/meta/app/main/images/TR-logo-hor-white.svg"/>
                    </a>
                    <span className="items-container">
                        { _(footerItems).map(item => <a href={item.href} className="footer-item">{item.title}</a>) }
                    </span>
                </div>
    }

}

define([], () => Footer)